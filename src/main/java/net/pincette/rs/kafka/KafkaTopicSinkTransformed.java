package net.pincette.rs.kafka;

import static net.pincette.rs.Mapper.map;

import java.time.Duration;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiFunction;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.TopicSink;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Connects Kafka topics to the <code>Streams</code> API on the producing side. It splits
 * serialization from the received type.
 *
 * @param <K> the key type.
 * @param <V> the serialized value type.
 * @param <T> the received key type.
 * @param <U> the received value type.
 * @author Werner Donné
 * @since 1.3.0
 */
public class KafkaTopicSinkTransformed<K, V, T, U>
    implements TopicSink<T, U, ProducerRecord<K, V>> {
  private final KafkaSubscriber<K, V> subscriber;
  private final BiFunction<String, Message<T, U>, ProducerRecord<K, V>> transformer;

  public KafkaTopicSinkTransformed(
      final KafkaSubscriber<K, V> subscriber,
      final BiFunction<String, Message<T, U>, ProducerRecord<K, V>> transformer) {
    this.subscriber = subscriber;
    this.transformer = transformer;
  }

  public Processor<Message<T, U>, ProducerRecord<K, V>> connect(final String topic) {
    return map(m -> transformer.apply(topic, m));
  }

  @Override
  public void stop(final Duration gracePeriod) {
    subscriber.stop(gracePeriod);
  }

  public Subscriber<ProducerRecord<K, V>> subscriber() {
    return subscriber.branch();
  }
}
