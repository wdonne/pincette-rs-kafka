package net.pincette.rs.kafka;

import static java.time.Instant.ofEpochMilli;
import static net.pincette.rs.Mapper.map;

import java.util.Map;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.TopicSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Connects Kafka topics to the <code>Streams</code> API on the consuming side. It splits
 * deserialization from the emitted type.
 *
 * @param <K> the key type.
 * @param <V> the deserialized value type.
 * @param <T> the emitted key type.
 * @param <U> the emitted value type.
 * @author Werner Donné
 * @since 1.3.0
 */
public class KafkaTopicSourceTransformed<K, V, T, U>
    implements TopicSource<T, U, ConsumerRecord<K, V>> {
  private final KafkaPublisher<K, V> publisher;
  private final Function<ConsumerRecord<K, V>, Message<T, U>> transformer;

  public KafkaTopicSourceTransformed(
      final KafkaPublisher<K, V> publisher,
      final Function<ConsumerRecord<K, V>, Message<T, U>> transformer) {
    this.publisher = publisher;
    this.transformer = transformer;
  }

  public Processor<ConsumerRecord<K, V>, Message<T, U>> connect(final String topic) {
    return map(r -> transformer.apply(r).withTimestamp(ofEpochMilli(r.timestamp())));
  }

  public Map<String, Publisher<ConsumerRecord<K, V>>> publishers() {
    return publisher.publishers();
  }

  public void start() {
    publisher.start();
  }

  public void stop() {
    publisher.stop();
  }
}
