package net.pincette.rs.kafka;

import static java.time.Instant.ofEpochMilli;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.streams.Message.message;

import java.util.Map;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.TopicSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaTopicSource<K, V> implements TopicSource<K, V, ConsumerRecord<K, V>> {
  private final KafkaPublisher<K, V> publisher;

  public KafkaTopicSource(final KafkaPublisher<K, V> publisher) {
    this.publisher = publisher;
  }

  public Processor<ConsumerRecord<K, V>, Message<K, V>> connect(final String topic) {
    return map(r -> message(r.key(), r.value()).withTimestamp(ofEpochMilli(r.timestamp())));
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
