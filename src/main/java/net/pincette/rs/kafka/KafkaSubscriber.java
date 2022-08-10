package net.pincette.rs.kafka;

import static java.time.Duration.ofMillis;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.logging.Level.SEVERE;
import static net.pincette.rs.Per.per;
import static net.pincette.rs.kafka.ProducerEvent.STOPPED;
import static net.pincette.rs.kafka.Util.LOGGER;
import static net.pincette.rs.kafka.Util.trace;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import net.pincette.util.Util.GeneralException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * This reactive streams subscriber writes the messages it receives to the Kafka topics they refer
 * to. It uses the Kafka producer that is given to it.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donn\u00e9
 */
public class KafkaSubscriber<K, V> implements Subscriber<ProducerRecord<K, V>> {
  private final BiConsumer<ProducerEvent, KafkaProducer<K, V>> eventHandler;
  private final Processor<ProducerRecord<K, V>, List<ProducerRecord<K, V>>> preprocessor =
      per(100, ofMillis(500));
  private final Supplier<KafkaProducer<K, V>> producerSupplier;
  private boolean completed;
  private CompletableFuture<Void> future;

  public KafkaSubscriber() {
    this(null, null);
  }

  private KafkaSubscriber(
      final Supplier<KafkaProducer<K, V>> producer,
      final BiConsumer<ProducerEvent, KafkaProducer<K, V>> eventHandler) {
    this.producerSupplier = producer;
    this.eventHandler = eventHandler;
    preprocessor.subscribe(new InternalSubscriber());
  }

  public static <K, V> KafkaSubscriber<K, V> subscriber(
      final Supplier<KafkaProducer<K, V>> producer) {
    return new KafkaSubscriber<>(producer, null);
  }

  /** This method blocks until the stream completes. */
  public void join() {
    future = new CompletableFuture<>();

    if (completed) {
      future.complete(null);
    }

    future.toCompletableFuture().join();
  }

  public void onComplete() {
    preprocessor.onComplete();
  }

  public void onError(final Throwable t) {
    preprocessor.onError(t);
  }

  public void onNext(final ProducerRecord<K, V> value) {
    preprocessor.onNext(value);
  }

  public void onSubscribe(final Subscription subscription) {
    preprocessor.onSubscribe(subscription);
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
    private boolean cancelled;
    private KafkaProducer<K, V> producer;
    private boolean sending;
    private Subscription subscription;

    private void close() {
      if (producer != null && !sending) {
        producer.close();
        producer = null;
      }
    }

    private void completeFuture() {
      if (future != null && (completed || cancelled)) {
        future.complete(null);
      }
    }

    private void end() {
      if (!cancelled) {
        cancelled = true;
        sendEvent(STOPPED);
        close();
        completeFuture();
      }
    }

    private Optional<KafkaProducer<K, V>> getProducer() {
      if (producer == null && !cancelled && !completed) {
        producer = producerSupplier.get();
      }

      return ofNullable(producer);
    }

    private void more() {
      if (!completed) {
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

      send(records).toCompletableFuture().join();
      more();
    }

    public void onSubscribe(final Subscription subscription) {
      this.subscription = subscription;
      sendEvent(ProducerEvent.STARTED);
      more();
    }

    private void panic(final Exception exception) {
      LOGGER.log(SEVERE, exception.getMessage(), exception);
      close();
    }

    private CompletionStage<Boolean> send(final List<ProducerRecord<K, V>> records) {
      return getProducer()
          .map(
              p -> {
                sending = true;

                for (int i = 0; i < records.size() - 1; ++i) {
                  p.send(trace(records.get(i)));
                }

                return sendToKafka(trace(records.get(records.size() - 1)))
                    .thenApply(
                        result -> {
                          sending = false;

                          if (completed) {
                            close();
                          }

                          return result;
                        });
              })
          .orElseGet(() -> failedFuture(new GeneralException("No producer")));
    }

    private void sendEvent(final ProducerEvent event) {
      if (eventHandler != null) {
        getProducer().ifPresent(p -> eventHandler.accept(event, p));
      }
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
