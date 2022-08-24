package net.pincette.rs.kafka;

import static java.time.Duration.ofSeconds;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.toSet;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.waitForCondition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import net.pincette.rs.streams.TopicSink;
import net.pincette.rs.streams.TopicSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Some utilities.
 *
 * @author Werner Donn\u00e9
 */
public class Util {
  private static final Duration INTERVAL = ofSeconds(1);
  static final Logger LOGGER = getLogger("net.pincette.rs.kafka");

  private Util() {}

  private static CompletionStage<Boolean> allTopicsAbsent(
      final Set<String> topics, final Admin admin) {
    return composeAsyncStream(topics.stream().map(topic -> topicAbsent(topic, admin)))
        .thenApply(results -> results.reduce((r1, r2) -> r1 && r2).orElse(true));
  }

  private static CompletionStage<Boolean> allTopicsPresent(
      final Set<String> topics, final Admin admin) {
    return admin
        .describeTopics(topics)
        .allTopicNames()
        .toCompletionStage()
        .thenApply(Map::keySet)
        .thenApply(t -> t.equals(topics))
        .exceptionally(e -> false);
  }

  /**
   * Completes when the topics are effectively available.
   *
   * @param topics the given topics.
   * @param admin the Kafka admin object.
   * @return The completion stage.
   */
  public static CompletionStage<Void> createTopics(final Set<NewTopic> topics, final Admin admin) {
    return admin
        .createTopics(new ArrayList<>(topics))
        .all()
        .toCompletionStage()
        .thenComposeAsync(
            r -> waitForTopicsPresent(topics.stream().map(NewTopic::name).collect(toSet()), admin));
  }

  /**
   * Completes when the topics are effectively deleted.
   *
   * @param topics the given topics.
   * @param admin the Kafka admin object.
   * @return The completion stage.
   */
  public static CompletionStage<Void> deleteTopics(final Set<String> topics, final Admin admin) {
    return presentTopics(topics, admin)
        .thenComposeAsync(t -> admin.deleteTopics(t).all().toCompletionStage().thenApply(r -> t))
        .thenComposeAsync(t -> waitForTopicsAbsent(t, admin));
  }

  private static CompletionStage<Boolean> describeTopic(
      final String topic,
      final BooleanSupplier found,
      final BooleanSupplier notFound,
      final Admin admin) {
    return admin
        .describeTopics(set(topic))
        .allTopicNames()
        .toCompletionStage()
        .thenApply(t -> found.getAsBoolean())
        .exceptionally(e -> notFound.getAsBoolean());
  }

  public static <K, V> Function<Set<String>, TopicSource<K, V, ConsumerRecord<K, V>>> fromPublisher(
      final KafkaPublisher<K, V> publisher) {
    return topics -> new KafkaTopicSource<>(publisher.withTopics(topics));
  }

  public static <K, V> Function<Set<String>, TopicSink<K, V, ProducerRecord<K, V>>> fromSubscriber(
      final KafkaSubscriber<K, V> subscriber) {
    return topics -> new KafkaTopicSink<>(subscriber);
  }

  private static CompletionStage<Set<String>> presentTopics(
      final Set<String> topics, final Admin admin) {
    return composeAsyncStream(
            topics.stream()
                .map(topic -> topicPresent(topic, admin).thenApply(result -> pair(topic, result))))
        .thenApply(pairs -> pairs.filter(p -> p.second).map(p -> p.first).collect(toSet()));
  }

  private static CompletionStage<Boolean> topicAbsent(final String topic, final Admin admin) {
    return describeTopic(topic, () -> false, () -> true, admin);
  }

  private static CompletionStage<Boolean> topicPresent(final String topic, final Admin admin) {
    return describeTopic(topic, () -> true, () -> false, admin);
  }

  static <T> T trace(final T value) {
    LOGGER.finest(value::toString);

    return value;
  }

  static <T> T trace(final String message, final T value) {
    LOGGER.finest(() -> message + ": " + value);

    return value;
  }

  static <T> T trace(final Supplier<String> message, final T value) {
    LOGGER.finest(() -> message.get() + ": " + value);

    return value;
  }

  private static CompletionStage<Boolean> waitFor(final Supplier<CompletionStage<Boolean>> check) {
    return net.pincette.util.Util.waitFor(waitForCondition(check), INTERVAL);
  }

  private static CompletionStage<Void> waitForTopicsAbsent(
      final Set<String> topics, final Admin admin) {
    return waitFor(() -> allTopicsAbsent(topics, admin)).thenAccept(r -> {});
  }

  private static CompletionStage<Void> waitForTopicsPresent(
      final Set<String> topics, final Admin admin) {
    return waitFor(() -> allTopicsPresent(topics, admin)).thenAccept(r -> {});
  }

  public static <K, V> ConsumerRecord<K, V> withKey(final ConsumerRecord<K, V> rec, final K key) {
    return new ConsumerRecord<>(rec.topic(), rec.partition(), rec.offset(), key, rec.value());
  }

  public static <K, V> ConsumerRecord<K, V> withValue(
      final ConsumerRecord<K, V> rec, final V value) {
    return new ConsumerRecord<>(rec.topic(), rec.partition(), rec.offset(), rec.key(), value);
  }
}
