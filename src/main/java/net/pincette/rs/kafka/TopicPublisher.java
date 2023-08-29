package net.pincette.rs.kafka;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.rs.Commit.commit;
import static net.pincette.rs.FlattenList.flattenList;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Serializer.dispatch;
import static net.pincette.rs.kafka.Util.trace;
import static net.pincette.util.Pair.pair;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import net.pincette.rs.Pipe;
import net.pincette.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The reactive streams publisher for a Kafka topic.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donné
 */
public class TopicPublisher<K, V> implements Publisher<ConsumerRecord<K, V>> {
  private final Queue<Pair<String, Boolean>> backpressure;
  private final Deque<List<ConsumerRecord<K, V>>> batches = new ArrayDeque<>(1000);
  private final Consumer<ConsumerRecord<K, V>> commit;
  private final Processor<List<ConsumerRecord<K, V>>, ConsumerRecord<K, V>> preprocessor =
      Pipe.<List<ConsumerRecord<K, V>>, ConsumerRecord<K, V>>pipe(flattenList())
          .then(map(Util::trace))
          .then(commit(this::commitRecords));
  private final String topic;
  private boolean cancelled;
  private boolean completed;
  private boolean more;
  private long requested;

  TopicPublisher(
      final String topic,
      final Consumer<ConsumerRecord<K, V>> commit,
      final Queue<Pair<String, Boolean>> backpressure) {
    this.topic = topic;
    this.commit = commit;
    this.backpressure = backpressure;
  }

  boolean cancelled() {
    return cancelled;
  }

  private CompletionStage<Boolean> commitRecords(final List<ConsumerRecord<K, V>> records) {
    trace(() -> "Publisher for topic " + topic + " receives " + records.size() + " to commit");
    records.forEach(commit);

    return completedFuture(true);
  }

  void complete() {
    dispatch(
        () -> {
          trace(() -> "Completing publisher for topic " + topic);
          completed = true;
          sendComplete();
        });
  }

  private void emit(final List<ConsumerRecord<K, V>> batch) {
    setMore(false);
    --requested;
    trace(() -> "Emit batch of size " + batch.size() + " from topic " + topic);
    preprocessor.onNext(batch);

    if (completed) {
      sendComplete();
    }
  }

  boolean more() {
    return more;
  }

  void next(final List<ConsumerRecord<K, V>> batch) {
    dispatch(
        () -> {
          if (!batch.isEmpty() && !cancelled()) {
            if (more()) {
              emit(batch);
            } else {
              trace(() -> "Buffer batch of size " + batch.size() + " from topic " + topic);
              batches.addFirst(batch);
            }
          }
        });
  }

  int queued() {
    return batches.size();
  }

  private void sendComplete() {
    if (cancelled || (requested > 0 && batches.isEmpty())) {
      trace(() -> "Publisher for topic " + topic + " sends onComplete");
      preprocessor.onComplete();
    }
  }

  private void setMore(final boolean more) {
    this.more = more;
    backpressure.add(pair(topic, more));
  }

  public void subscribe(final Subscriber<? super ConsumerRecord<K, V>> subscriber) {
    preprocessor.subscribe(subscriber);
    preprocessor.onSubscribe(new Backpressure());
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      dispatch(
          () -> {
            cancelled = true;
            setMore(false);
            complete();
          });
    }

    public void request(final long n) {
      dispatch(
          () -> {
            requested += n;

            if (completed) {
              sendComplete();
            } else {
              sendOneBatch();
            }
          });
    }

    private void sendOneBatch() {
      ofNullable(batches.pollLast())
          .ifPresentOrElse(TopicPublisher.this::emit, () -> setMore(!cancelled() && !completed));
    }
  }
}
