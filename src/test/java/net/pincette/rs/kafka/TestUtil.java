package net.pincette.rs.kafka;

import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.logging.LogManager.getLogManager;
import static java.util.stream.Collectors.toSet;
import static net.pincette.rs.Util.onComplete;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDoRethrow;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import net.pincette.util.State;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

class TestUtil {
  static {
    tryToDoRethrow(
        () ->
            getLogManager()
                .readConfiguration(TestUtil.class.getResourceAsStream("/logging.properties")));
  }

  static final Map<String, Object> COMMON_CONFIG =
      map(pair(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
  static final Map<String, Object> CONSUMER_CONFIG =
      merge(
          COMMON_CONFIG,
          map(
              pair(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
              pair(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
              pair(GROUP_ID_CONFIG, "test-" + randomUUID()),
              pair(ENABLE_AUTO_COMMIT_CONFIG, false)));
  static final Map<String, Object> PRODUCER_CONFIG =
      merge(
          COMMON_CONFIG,
          map(
              pair(ACKS_CONFIG, "all"),
              pair(ENABLE_IDEMPOTENCE_CONFIG, true),
              pair(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
              pair(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));

  private TestUtil() {}

  static boolean checkOffsets(
      final Map<TopicPartition, Long> start,
      final Map<TopicPartition, Long> end,
      final BiPredicate<Long, Long> test) {
    return start.size() == end.size()
        && start.keySet().equals(end.keySet())
        && start.entrySet().stream().allMatch(e -> test.test(e.getValue(), end.get(e.getKey())));
  }

  static KafkaConsumer<String, String> consumer() {
    return new KafkaConsumer<>(CONSUMER_CONFIG);
  }

  static void measure(final Runnable run) {
    final Instant started = now();

    run.run();
    System.out.println("Took " + between(started, now()).getSeconds() + " seconds");
  }

  static NewTopic newTopic(final String name) {
    return new NewTopic(name, 1, (short) 1);
  }

  static <K, V> Map<TopicPartition, Long> offsets(
      final KafkaConsumer<K, V> consumer, final Set<String> topics) {
    return map(
        consumer
            .committed(
                topics.stream()
                    .flatMap(t -> consumer.partitionsFor(t).stream())
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .collect(toSet()))
            .entrySet()
            .stream()
            .map(
                e ->
                    pair(
                        e.getKey(),
                        ofNullable(e.getValue()).map(OffsetAndMetadata::offset).orElse(-1L))));
  }

  static KafkaProducer<String, String> producer() {
    return new KafkaProducer<>(PRODUCER_CONFIG);
  }

  static <K, V> BiConsumer<ConsumerEvent, KafkaConsumer<K, V>> producerEventHandler(
      final State<Map<TopicPartition, Long>> startOffsets,
      final Set<String> topics,
      final BiPredicate<Long, Long> offsetTest) {
    return (event, consumer) -> {
      if (event == ConsumerEvent.STARTED) {
        consumer.seekToBeginning(consumer.assignment());
        startOffsets.set(positions(consumer, topics));
      } else if (event == ConsumerEvent.STOPPED) {
        assertTrue(checkOffsets(startOffsets.get(), offsets(consumer, topics), offsetTest));
      }
    };
  }

  private static <K, V> Map<TopicPartition, Long> positions(
      final KafkaConsumer<K, V> consumer, final Set<String> topics) {
    return map(
        topics.stream()
            .flatMap(t -> consumer.partitionsFor(t).stream())
            .map(p -> new TopicPartition(p.topic(), p.partition()))
            .map(p -> pair(p, consumer.position(p))));
  }

  static Subscriber<? super Integer> testComplete(
      final Runnable stop, final AtomicInteger stopped) {
    return onComplete(
        () -> {
          if (stopped.decrementAndGet() == 0) {
            stop.run();
          }
        });
  }

  static String topic(final String name) {
    return name + "-" + randomUUID();
  }
}
