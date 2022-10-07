package net.pincette.rs.kafka;

import static java.lang.Boolean.FALSE;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.logging.Level.SEVERE;
import static net.pincette.rs.Per.per;
import static net.pincette.rs.kafka.ProducerEvent.STOPPED;
import static net.pincette.rs.kafka.Util.LOGGER;
import static net.pincette.rs.kafka.Util.trace;
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
import java.util.function.Supplier;
import net.pincette.function.SideEffect;
import net.pincette.util.State;
import net.pincette.util.Util.GeneralException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * This reactive streams subscriber writes the messages it receives to the Kafka topics they refer
 * to. It uses the Kafka producer that is given to it. Use this subscriber directly if you want to
 * subscribe to only one publisher. Use the <code>branch</code> method if you want to subscribe to
 * multiple publishers. They will share the producer.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donn\u00e9
 */
public class KafkaSubscriber<K, V> implements Subscriber<ProducerRecord<K, V>> {
  private static final Duration BACKOFF = ofSeconds(5);
  private static final int BATCH = 500;
  private static final Duration TIMEOUT = ofMillis(500);
  private static final Duration WAIT_INTERVAL = ofMillis(500);

  private final BiConsumer<ProducerEvent, KafkaProducer<K, V>> eventHandler;
  private final List<InternalSubscriber> internalSubscribers = new ArrayList<>();
  private final Supplier<KafkaProducer<K, V>> producerSupplier;
  private Subscriber<ProducerRecord<K, V>> branch;
  private KafkaProducer<K, V> producer;
  private boolean sending;
  private Subscription subscription;

  public KafkaSubscriber() {
    this(null, null);
  }

  private KafkaSubscriber(
      final Supplier<KafkaProducer<K, V>> producer,
      final BiConsumer<ProducerEvent, KafkaProducer<K, V>> eventHandler) {
    this.producerSupplier = producer;
    this.eventHandler = eventHandler;
  }

  public static <K, V> KafkaSubscriber<K, V> subscriber(
      final Supplier<KafkaProducer<K, V>> producer) {
    return new KafkaSubscriber<>(producer, null);
  }

  private boolean allCancelled() {
    return internalSubscribers.stream().allMatch(s -> s.cancelled);
  }

  public Subscriber<ProducerRecord<K, V>> branch() {
    final Processor<ProducerRecord<K, V>, List<ProducerRecord<K, V>>> preprocessor =
        per(BATCH, TIMEOUT, TIMEOUT);
    final InternalSubscriber subscriber = new InternalSubscriber();

    internalSubscribers.add(subscriber);
    preprocessor.subscribe(subscriber);

    return preprocessor;
  }

  private void cancelAll() {
    internalSubscribers.stream().filter(s -> !s.cancelled).forEach(InternalSubscriber::cancel);
  }

  private int cancelled() {
    return (int) internalSubscribers.stream().filter(s -> s.cancelled).count();
  }

  private void close(boolean force) {
    if (producer != null && (!sending || force)) {
      final KafkaProducer<K, V> p = producer;

      LOGGER.finest(() -> "Closing producer " + p);
      producer = null;
      p.close();
    }
  }

  private Optional<KafkaProducer<K, V>> getProducer() {
    if (producer == null && !allCancelled()) {
      producer = producerSupplier.get();
    }

    return ofNullable(producer);
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
      getProducer().ifPresent(p -> eventHandler.accept(event, p));
    }
  }

  /**
   * Stops the subscriber. This method blocks until all branch subscribers are stopped.
   *
   * @param gracePeriod the period after which the branch subscriptions that didn't complete
   *     naturally will be cancelled.
   */
  @SuppressWarnings("java:S106") // At this point all loggers are dead already.
  public void stop(final Duration gracePeriod) {
    if (!allCancelled()) {
      waitForCancel(gracePeriod)
          .thenAccept(
              result -> {
                if (FALSE.equals(result)) {
                  System.out.println(
                      " Have to cancel "
                          + internalSubscribers.stream().filter(s -> !s.cancelled).count()
                          + " of "
                          + internalSubscribers.size());
                }

                cancelAll();
                stop();
              });
    } else {
      stop();
    }

    join();
  }

  private void stop() {
    if (allCancelled()) {
      sendEvent(STOPPED);
      close(false);
    }
  }

  private CompletionStage<Boolean> waitForCancel(final Duration gracePeriod) {
    final State<Integer> cancelled = new State<>(cancelled());

    return waitFor(
            waitForCondition(() -> completedFuture(allCancelled())),
            () ->
                Optional.of(cancelled())
                    .filter(c -> c > cancelled.get())
                    .map(
                        c -> SideEffect.<Boolean>run(() -> cancelled.set(c)).andThenGet(() -> true))
                    .orElse(false),
            WAIT_INTERVAL,
            gracePeriod)
        .thenApply(result -> result.orElse(false));
  }

  /**
   * Creates a subscriber with an event handler that receives lifecycle events from the subscriber.
   *
   * @param eventHandler the function that consumes the events. It may be <code>null</code>.
   * @return A new subscriber instance.
   */
  public KafkaSubscriber<K, V> withEventHandler(
      final BiConsumer<ProducerEvent, KafkaProducer<K, V>> eventHandler) {
    return new KafkaSubscriber<>(producerSupplier, eventHandler);
  }

  /**
   * Creates a subscriber with a Kafka producer function. It may be called several times, for
   * example when there are issues with the Kafka cluster.
   *
   * @param producer the function to generate a new Kafka producer.
   * @return A new subscriber instance.
   */
  public KafkaSubscriber<K, V> withProducer(final Supplier<KafkaProducer<K, V>> producer) {
    return new KafkaSubscriber<>(producer, eventHandler);
  }

  private class InternalSubscriber implements Subscriber<List<ProducerRecord<K, V>>> {
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private boolean cancelled;
    private boolean completed;
    private Subscription subscription;

    private void cancel() {
      cancelled = true;
      subscription.cancel();
      completeFuture();
    }

    private void completeFuture() {
      if (completed || cancelled) {
        future.complete(null);
      }
    }

    private void end() {
      if (!cancelled) {
        cancelled = true;
        stop();
        completeFuture();
      }
    }

    private void more() {
      if (!completed && !cancelled) {
        subscription.request(1);
      }
    }

    public void onComplete() {
      completed = true;
      end();
    }

    public void onError(final Throwable t) {
      LOGGER.log(SEVERE, t.getMessage(), t);
      sendEvent(ProducerEvent.ERROR);
      end();
    }

    public void onNext(final List<ProducerRecord<K, V>> records) {
      if (completed) {
        throw new GeneralException("onNext on completed stream");
      }

      LOGGER.finest(() -> "Sending batch of size " + records.size() + " to Kafka");
      tryToGetForever(() -> send(records), BACKOFF, this::panic).toCompletableFuture().join();
      more();
    }

    public void onSubscribe(final Subscription subscription) {
      if (this.subscription != null) {
        this.subscription.cancel();
      }

      this.subscription = subscription;
      sendEvent(ProducerEvent.STARTED);
      more();
    }

    private void panic(final Exception exception) {
      LOGGER.log(SEVERE, exception.getMessage(), exception);
      close(true);
    }

    private CompletionStage<Boolean> send(final List<ProducerRecord<K, V>> records) {
      return getProducer()
          .map(
              p -> {
                sending = true;

                for (int i = 0; i < records.size() - 1; ++i) {
                  p.send(trace("Send record", records.get(i)));
                }

                return sendToKafka(trace("Send record", records.get(records.size() - 1)))
                    .thenApply(
                        result -> {
                          sending = false;

                          if (completed) {
                            end();
                          }

                          return result;
                        });
              })
          .orElseGet(
              () ->
                  cancelled
                      ? completedFuture(false)
                      : failedFuture(new GeneralException("No producer")));
    }

    private CompletionStage<Boolean> sendToKafka(final ProducerRecord<K, V> rec) {
      final CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

      getProducer()
          .ifPresentOrElse(
              p ->
                  p.send(
                      rec,
                      (metadata, exception) -> {
                        if (exception != null) {
                          panic(exception);
                          completableFuture.complete(false);
                        } else {
                          completableFuture.complete(true);
                        }
                      }),
              () -> completableFuture.completeExceptionally(new GeneralException("No producer")));

      return completableFuture;
    }
  }
}
