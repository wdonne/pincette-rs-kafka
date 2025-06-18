package net.pincette.rs.kafka;

import static net.pincette.rs.Mapper.map;

import java.time.Duration;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.TopicSink;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Connects Kafka topics to the <code>Streams</code> API on the producing side.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donné
 */
public class KafkaTopicSink<K, V> implements TopicSink<K, V, ProducerRecord<K, V>> {
  private final KafkaSubscriber<K, V> subscriber;

  public KafkaTopicSink(final KafkaSubscriber<K, V> subscriber) {
    this.subscriber = subscriber;
  }

  public Processor<Message<K, V>, ProducerRecord<K, V>> connect(final String topic) {
    return map(m -> new ProducerRecord<>(topic, m.key, m.value));
  }

  @Override
  public void stop(final Duration gracePeriod) {
    subscriber.stop(gracePeriod);
  }

  public Subscriber<ProducerRecord<K, V>> subscriber() {
    return subscriber.branch();
  }
}
