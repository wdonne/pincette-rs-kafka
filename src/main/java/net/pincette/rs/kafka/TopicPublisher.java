package net.pincette.rs.kafka;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.rs.Commit.commit;
import static net.pincette.rs.FlattenList.flattenList;
import static net.pincette.rs.Mapper.map;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import net.pincette.function.SideEffect;
import net.pincette.rs.Pipe;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The reactive streams publisher for a Kafka topic.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donn\u00e9
 */
public class TopicPublisher<K, V> implements Publisher<ConsumerRecord<K, V>> {
  private final Deque<List<ConsumerRecord<K, V>>> batches = new ConcurrentLinkedDeque<>();
  private final Consumer<String> cancel;
  private final Consumer<ConsumerRecord<K, V>> commit;
  private final Processor<List<ConsumerRecord<K, V>>, ConsumerRecord<K, V>> preprocessor =
      Pipe.<List<ConsumerRecord<K, V>>, ConsumerRecord<K, V>>pipe(flattenList())
          .then(map(Util::trace))
          .then(commit(this::commitRecords));
  private final String topic;
  private boolean completed;
  private boolean more;

  TopicPublisher(
      final String topic,
      final Consumer<ConsumerRecord<K, V>> commit,
      final Consumer<String> cancel) {
    this.topic = topic;
    this.commit = commit;
    this.cancel = cancel;
  }

  private CompletionStage<Boolean> commitRecords(final List<ConsumerRecord<K, V>> records) {
    records.forEach(commit);

    return completedFuture(true);
  }

  void complete() {
    completed = true;
    more = false;

    while (ofNullable(batches.pollLast())
        .map(b -> SideEffect.<Boolean>run(() -> emit(b)).andThenGet(() -> true))
        .isPresent())
      ;

    preprocessor.onComplete();
  }

  private void emit(final List<ConsumerRecord<K, V>> batch) {
    more = false;
    preprocessor.onNext(batch);
  }

  boolean more() {
    return more;
  }

  void next(final List<ConsumerRecord<K, V>> batch) {
    if (more()) {
      emit(batch);
    } else {
      batches.addFirst(batch);
    }
  }

  public void subscribe(final Subscriber<? super ConsumerRecord<K, V>> subscriber) {
    preprocessor.subscribe(subscriber);
    preprocessor.onSubscribe(new Backpressure());
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      TopicPublisher.this.cancel.accept(topic);
    }

    public void request(final long n) {
      if (!completed) {
        ofNullable(batches.pollLast())
            .ifPresentOrElse(TopicPublisher.this::emit, () -> more = true);
      }
    }
  }
}
