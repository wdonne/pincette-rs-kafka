package net.pincette.rs.kafka;

import net.pincette.rs.Mapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Converts Kafka consumer records to Kafka producer records.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donn\u00e9
 */
public class ToTopic<K, V> extends Mapper<ConsumerRecord<K, V>, ProducerRecord<K, V>> {
  public ToTopic(final String topic) {
    super(b -> translate(b, topic));
  }

  public static <K, V> ToTopic<K, V> toTopic(final String topic) {
    return new ToTopic<>(topic);
  }

  private static <K, V> ProducerRecord<K, V> translate(
      final ConsumerRecord<K, V> rec, final String topic) {
    return new ProducerRecord<>(topic, rec.key(), rec.value());
  }
}
