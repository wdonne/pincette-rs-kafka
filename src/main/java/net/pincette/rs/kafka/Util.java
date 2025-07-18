package net.pincette.rs.kafka;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.System.currentTimeMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.toSet;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.tryToGetForever;
import static net.pincette.util.Util.waitForCondition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.TopicSink;
import net.pincette.rs.streams.TopicSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Some utilities.
 *
 * @author Werner Donné
 */
public class Util {
  private static final Duration INTERVAL = ofSeconds(1);
  static final Logger LOGGER = getLogger("net.pincette.rs.kafka");

  private Util() {}

  private static CompletionStage<Boolean> allAbsent(
      final Set<String> resources, final Function<String, CompletionStage<Boolean>> test) {
    return composeAsyncStream(resources.stream().map(test))
        .thenApply(results -> results.reduce((r1, r2) -> r1 && r2).orElse(true));
  }

  private static CompletionStage<Boolean> allConsumerGroupsAbsent(
      final Set<String> groupIds, final Admin admin) {
    return allAbsent(groupIds, groupId -> consumerGroupAbsent(groupId, admin));
  }

  private static CompletionStage<Boolean> allTopicsAbsent(
      final Set<String> topics, final Admin admin) {
    return allAbsent(topics, topic -> topicAbsent(topic, admin));
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

  private static CompletionStage<Boolean> consumerGroupAbsent(
      final String groupId, final Admin admin) {
    return admin
        .listConsumerGroupOffsets(groupId)
        .all()
        .toCompletionStage()
        .thenApply(r -> noOffsets(r, groupId))
        .exceptionally(e -> true);
  }

  /**
   * Completes when the topics are effectively available.
   *
   * @param topics the given topics.
   * @param admin the Kafka admin object.
   * @return The completion stage.
   */
  public static CompletionStage<Void> createTopics(final Set<NewTopic> topics, final Admin admin) {
    return tryToGetForever(
            () ->
                admin
                    .createTopics(new ArrayList<>(topics))
                    .all()
                    .toCompletionStage()
                    .thenComposeAsync(
                        r ->
                            waitForTopicsPresent(
                                topics.stream().map(NewTopic::name).collect(toSet()), admin))
                    .thenApply(r -> true),
            INTERVAL,
            e -> deleteTopics(topics.stream().map(NewTopic::name).collect(toSet()), admin))
        .thenAccept(r -> {});
  }

  /**
   * Completes when the consumer groups are effectively deleted.
   *
   * @param groupIds the given consumer groups.
   * @param admin the Kafka admin object.
   * @return The completion stage.
   * @since 1.3.1
   */
  public static CompletionStage<Void> deleteConsumerGroups(
      final Set<String> groupIds, final Admin admin) {
    return tryToGetForever(
            () ->
                admin
                    .deleteConsumerGroups(
                        groupIds, new DeleteConsumerGroupsOptions().timeoutMs(MAX_VALUE))
                    .all()
                    .toCompletionStage()
                    .thenApply(r -> groupIds)
                    .thenComposeAsync(g -> waitForConsumerGroupsAbsent(g, admin))
                    .thenApply(r -> true)
                    .exceptionally(e -> true),
            INTERVAL)
        .thenAccept(r -> {});
  }

  /**
   * Completes when the topics are effectively deleted.
   *
   * @param topics the given topics.
   * @param admin the Kafka admin object.
   * @return The completion stage.
   */
  public static CompletionStage<Void> deleteTopics(final Set<String> topics, final Admin admin) {
    return tryToGetForever(
            () ->
                presentTopics(topics, admin)
                    .thenComposeAsync(
                        t ->
                            admin
                                .deleteTopics(t, new DeleteTopicsOptions().timeoutMs(MAX_VALUE))
                                .all()
                                .toCompletionStage()
                                .thenApply(r -> t))
                    .thenComposeAsync(t -> waitForTopicsAbsent(t, admin))
                    .thenApply(r -> true),
            INTERVAL)
        .thenAccept(r -> {});
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

  public static <K, V, T, U>
      Function<Set<String>, TopicSource<T, U, ConsumerRecord<K, V>>> fromPublisherTransformed(
          final KafkaPublisher<K, V> publisher,
          final Function<ConsumerRecord<K, V>, Message<T, U>> transformer) {
    return topics -> new KafkaTopicSourceTransformed<>(publisher.withTopics(topics), transformer);
  }

  public static <K, V> Function<Set<String>, TopicSink<K, V, ProducerRecord<K, V>>> fromSubscriber(
      final KafkaSubscriber<K, V> subscriber) {
    return topics -> new KafkaTopicSink<>(subscriber);
  }

  public static <K, V, T, U>
      Function<Set<String>, TopicSink<T, U, ProducerRecord<K, V>>> fromSubscriberTransformed(
          final KafkaSubscriber<K, V> subscriber,
          final BiFunction<String, Message<T, U>, ProducerRecord<K, V>> transformer) {
    return topics -> new KafkaTopicSinkTransformed<>(subscriber, transformer);
  }

  private static boolean noOffsets(
      final Map<String, Map<TopicPartition, OffsetAndMetadata>> offsets, final String groupId) {
    return offsets.isEmpty()
        || ofNullable(offsets.get(groupId))
            .filter(m -> !m.isEmpty() && m.values().stream().anyMatch(Objects::nonNull))
            .isEmpty();
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
    return trace(() -> message, value);
  }

  static <T> T trace(final Supplier<String> message, final T value) {
    LOGGER.finest(() -> currentTimeMillis() + ": " + message.get() + ": " + value);

    return value;
  }

  static void trace(final Supplier<String> message) {
    LOGGER.finest(() -> currentTimeMillis() + ": " + message.get());
  }

  private static CompletionStage<Boolean> waitFor(final Supplier<CompletionStage<Boolean>> check) {
    return net.pincette.util.Util.waitFor(waitForCondition(check), INTERVAL);
  }

  private static CompletionStage<Void> waitForConsumerGroupsAbsent(
      final Set<String> groupIds, final Admin admin) {
    return waitFor(() -> allConsumerGroupsAbsent(groupIds, admin)).thenAccept(r -> {});
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
