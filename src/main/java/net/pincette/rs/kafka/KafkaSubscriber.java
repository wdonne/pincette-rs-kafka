package net.pincette.rs.kafka;

import static java.lang.Boolean.FALSE;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static net.pincette.rs.Per.per;
import static net.pincette.rs.kafka.ProducerEvent.STOPPED;
import static net.pincette.rs.kafka.Util.LOGGER;
import static net.pincette.rs.kafka.Util.trace;
import static net.pincette.util.Util.rethrow;
import static net.pincette.util.Util.tryToGetForever;
import static net.pincette.util.Util.waitFor;
import static net.pincette.util.Util.waitForCondition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import net.pincette.function.SideEffect;
import net.pincette.rs.Serializer;
import net.pincette.util.State;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;

/**
 * This reactive streams subscriber writes the messages it receives to the Kafka topics they refer
 * to. It uses the Kafka producer that is given to it. Use this subscriber directly if you want to
 * subscribe to only one publisher. Use the <code>branch</code> method if you want to subscribe to
 * multiple publishers. They will share the producer.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donné
 */
public class KafkaSubscriber<K, V> implements Subscriber<ProducerRecord<K, V>> {
  private static final Duration BACKOFF = ofSeconds(5);
  private static final int BATCH = 500;
  private static final Duration DEFAULT_TIMEOUT = ofMillis(50);
  private static final Duration WAIT_INTERVAL = ofMillis(500);

  private final int batchSize;
  private final BiConsumer<ProducerEvent, KafkaProducer<K, V>> eventHandler;
  private final List<InternalSubscriber> internalSubscribers = new ArrayList<>();
  private final Supplier<KafkaProducer<K, V>> producerSupplier;
  private final Duration timeout;
  private Subscriber<ProducerRecord<K, V>> branch;
  private KafkaProducer<K, V> producer;
  private Subscription subscription;

  public KafkaSubscriber() {
    this(null, null, BATCH, DEFAULT_TIMEOUT);
  }

  private KafkaSubscriber(
      final Supplier<KafkaProducer<K, V>> producer,
      final BiConsumer<ProducerEvent, KafkaProducer<K, V>> eventHandler,
      final int batchSize,
      final Duration timeout) {
    this.producerSupplier = producer;
    this.eventHandler = eventHandler;
    this.batchSize = batchSize;
    this.timeout = timeout;
  }

  private static <K, V> void handleKafkaException(
      final ProducerRecord<K, V> rec, final Exception e) {
    if (e != null) {
      if (e instanceof RecordTooLargeException) {
        reportTooLarge(rec);
      } else {
        rethrow(e);
      }
    }
  }

  private static <K, V> void reportTooLarge(final ProducerRecord<K, V> rec) {
    LOGGER.log(
        SEVERE,
        "The record with key {0} could not be written to the topic {1} because it is too large. "
            + "Its value starts with {2}.",
        new Object[] {rec.key(), rec.topic(), truncate(rec.value().toString(), 4096)});
  }

  public static <K, V> KafkaSubscriber<K, V> subscriber(
      final Supplier<KafkaProducer<K, V>> producer) {
    return new KafkaSubscriber<>(producer, null, BATCH, DEFAULT_TIMEOUT);
  }

  private static String truncate(final String s, final int size) {
    return s.length() > size ? s.substring(0, size) : s;
  }

  private boolean allCompleted() {
    return allCondition(s -> s.completed);
  }

  private boolean allCondition(final Predicate<InternalSubscriber> condition) {
    return internalSubscribers.stream().allMatch(condition);
  }

  public Subscriber<ProducerRecord<K, V>> branch() {
    final Processor<ProducerRecord<K, V>, List<ProducerRecord<K, V>>> preprocessor =
        per(batchSize, DEFAULT_TIMEOUT);
    final InternalSubscriber subscriber = new InternalSubscriber();

    internalSubscribers.add(subscriber);
    preprocessor.subscribe(subscriber);

    return preprocessor;
  }

  private int completed() {
    return countCondition(s -> s.completed);
  }

  private int countCondition(final Predicate<InternalSubscriber> condition) {
    return (int) internalSubscribers.stream().filter(condition).count();
  }

  /** This method blocks until all the upstream publishers complete. */
  public void join() {
    allOf(internalSubscribers.stream().map(s -> s.future).toArray(CompletableFuture[]::new)).join();
  }

  public void onComplete() {
    branch.onComplete();
  }

  public void onError(final Throwable t) {
    branch.onError(t);
  }

  public void onNext(final ProducerRecord<K, V> value) {
    branch.onNext(value);
  }

  public void onSubscribe(final Subscription subscription) {
    if (this.subscription != null) {
      this.subscription.cancel();
    }

    this.subscription = subscription;
    branch = branch();
    branch.onSubscribe(subscription);
  }

  private void sendEvent(final ProducerEvent event) {
    if (eventHandler != null) {
      eventHandler.accept(event, producer);
    }
  }

  private void setCompleted() {
    internalSubscribers.stream().filter(s -> !s.completed).forEach(InternalSubscriber::onComplete);
  }

  /**
   * Stops the subscriber. This method blocks until all branch subscribers are stopped.
   *
   * @param gracePeriod the period after which the branch subscriptions that didn't complete
   *     naturally will be cancelled.
   */
  @SuppressWarnings("java:S106") // At this point all loggers are dead already.
  public void stop(final Duration gracePeriod) {
    if (!allCompleted()) {
      waitForCompleted(gracePeriod)
          .thenAccept(
              result -> {
                if (FALSE.equals(result)) {
                  System.out.println(
                      " "
                          + (internalSubscribers.size() - completed())
                          + " branches "
                          + "didn't complete");
                }

                setCompleted();
                stop();
              });
    } else {
      stop();
    }

    join();
  }

  private void stop() {
    if (allCompleted()) {
      sendEvent(STOPPED);
      producer.close();
    }
  }

  private CompletionStage<Boolean> waitForCompleted(final Duration gracePeriod) {
    final State<Integer> completed = new State<>(completed());

    return waitFor(
            waitForCondition(() -> completedFuture(allCompleted())),
            () ->
                Optional.of(completed())
                    .filter(c -> c > completed.get())
                    .map(
                        c -> SideEffect.<Boolean>run(() -> completed.set(c)).andThenGet(() -> true))
                    .orElse(false),
            WAIT_INTERVAL,
            gracePeriod)
        .thenApply(result -> result.orElse(false));
  }

  /**
   * Creates a subscriber with a given batch size. The default is 500.
   *
   * @param batchSize the size of the batch the subscriber is requesting.
   * @return A new subscriber instance.
   */
  public KafkaSubscriber<K, V> withBatchSize(final int batchSize) {
    return new KafkaSubscriber<>(producerSupplier, eventHandler, batchSize, timeout);
  }

  /**
   * Creates a subscriber with an event handler that receives lifecycle events from the subscriber.
   *
   * @param eventHandler the function that consumes the events. It may be <code>null</code>.
   * @return A new subscriber instance.
   */
  public KafkaSubscriber<K, V> withEventHandler(
      final BiConsumer<ProducerEvent, KafkaProducer<K, V>> eventHandler) {
    return new KafkaSubscriber<>(producerSupplier, eventHandler, batchSize, timeout);
  }

  /**
   * Creates a subscriber with a Kafka producer function. It may be called several times, for
   * example when there are issues with the Kafka cluster.
   *
   * @param producer the function to generate a new Kafka producer.
   * @return A new subscriber instance.
   */
  public KafkaSubscriber<K, V> withProducer(final Supplier<KafkaProducer<K, V>> producer) {
    return new KafkaSubscriber<>(producer, eventHandler, batchSize, timeout);
  }

  /**
   * Creates a subscriber with a given timeout. The default is 50ms.
   *
   * @param timeout the time after which a batch that is not yet full is flushed and additional
   *     messages are requested.
   * @return A new subscriber instance.
   */
  public KafkaSubscriber<K, V> withTimeout(final Duration timeout) {
    return new KafkaSubscriber<>(producerSupplier, eventHandler, batchSize, timeout);
  }

  private class InternalSubscriber implements Subscriber<List<ProducerRecord<K, V>>> {
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private boolean cancelled;
    private boolean completed;
    private final String key = randomUUID().toString();
    private Subscription subscription;

    private void completeFuture() {
      if (completed || cancelled) {
        future.complete(null);
      }
    }

    private void dispatch(final Runnable run) {
      Serializer.dispatch(run, key);
    }

    private void end() {
      if (!cancelled) {
        cancelled = true;
        stop();
        completeFuture();
      }
    }

    private void more() {
      dispatch(
          () -> {
            if (!completed && !cancelled) {
              subscription.request(1);
            }
          });
    }

    public void onComplete() {
      dispatch(
          () -> {
            completed = true;
            end();
          });
    }

    public void onError(final Throwable t) {
      LOGGER.log(SEVERE, t.getMessage(), t);
      sendEvent(ProducerEvent.ERROR);
      dispatch(this::end);
    }

    public void onNext(final List<ProducerRecord<K, V>> records) {
      dispatch(
          () -> {
            if (!completed) { // Completion can be enforced when the subscriber is stopped.
              trace(() -> "Sending batch of size " + records.size() + " to Kafka");
              tryToGetForever(() -> send(records), BACKOFF, this::panic).thenAccept(r -> more());
            }
          });
    }

    public void onSubscribe(final Subscription subscription) {
      if (this.subscription != null) {
        this.subscription.cancel();
      }

      if (producer == null) {
        producer = producerSupplier.get();
      }

      this.subscription = subscription;
      sendEvent(ProducerEvent.STARTED);
      more();
    }

    private void panic(final Exception exception) {
      LOGGER.log(SEVERE, exception.getMessage(), exception);
      producer.close();
      producer = producerSupplier.get();
    }

    private CompletionStage<Boolean> send(final List<ProducerRecord<K, V>> records) {
      sendBatchPrefix(records, producer);

      return sendToKafka(trace("Send record", records.getLast()))
          .thenApply(
              result -> {
                if (completed) {
                  end();
                }

                return result;
              });
    }

    private void sendBatchPrefix(
        final List<ProducerRecord<K, V>> records, final KafkaProducer<K, V> producer) {
      for (int i = 0; i < records.size() - 1; ++i) {
        final ProducerRecord<K, V> rec = records.get(i);

        producer.send(
            trace("Send record", rec),
            (metadata, exception) -> handleKafkaException(rec, exception));
      }
    }

    private CompletionStage<Boolean> sendToKafka(final ProducerRecord<K, V> rec) {
      final CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

      producer.send(
          rec,
          (metadata, exception) -> {
            if (exception != null && !(exception instanceof RecordTooLargeException)) {
              panic(exception);
              completableFuture.complete(false);
            } else {
              if (exception != null) {
                reportTooLarge(rec);
              }

              completableFuture.complete(true);
            }
          });

      return completableFuture;
    }
  }
}
