package net.pincette.rs.kafka;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.rs.kafka.Util.LOGGER;
import static net.pincette.rs.kafka.Util.trace;
import static net.pincette.util.Collections.consumeHead;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.Util.tryToDo;
import static net.pincette.util.Util.tryToGetForever;

import java.time.Duration;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import net.pincette.function.SideEffect;
import net.pincette.util.State;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * This publisher exposes an actual reactive streams publisher for each topic it is asked to
 * consume. You give it a function to generate a Kafka consumer, which it will use to do the actual
 * consumption. The publisher manages offset commits itself, so you shouldn't use the auto-commit
 * feature of the Kafka consumer. This publisher supports "at least once" semantics, which implies
 * crashes may be followed by duplicate messages. Backpressure is managed by pausing topics when
 * needed. This will not cause a Kafka rebalance.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Werner Donn\u00e9
 */
public class KafkaPublisher<K, V> {
  private static final Duration POLL_TIMEOUT = ofMillis(100);
  private static final Duration RETRY = ofSeconds(5);
  private static final int WAIT_TIMEOUT = 3000;

  private final Set<String> cancelled = new HashSet<>();
  private final Supplier<KafkaConsumer<K, V>> consumerSupplier;
  private final BiConsumer<ConsumerEvent, KafkaConsumer<K, V>> eventHandler;
  private final Map<TopicPartition, OffsetAndMetadata> pendingCommits = new ConcurrentHashMap<>();
  private final Set<String> paused = new HashSet<>();
  private final Map<String, TopicPublisher<K, V>> publishers;
  private final Deque<ConsumerRecord<K, V>> recordsToCommit = new ConcurrentLinkedDeque<>();
  private final Set<String> topics;
  private KafkaConsumer<K, V> consumer;
  private boolean started;
  private boolean stop;

  /**
   * Creates an uninitialized publisher. Use the <code>with</code> methods to produce initialized
   * instances.
   */
  public KafkaPublisher() {
    this(null, null, null);
  }

  private KafkaPublisher(
      final Supplier<KafkaConsumer<K, V>> consumer,
      final Set<String> topics,
      final BiConsumer<ConsumerEvent, KafkaConsumer<K, V>> eventHandler) {
    this.consumerSupplier = consumer;
    this.topics = topics;
    this.eventHandler = eventHandler;
    publishers = createPublishers();
  }

  public static <K, V> KafkaPublisher<K, V> publisher(
      final Supplier<KafkaConsumer<K, V>> consumer) {
    return new KafkaPublisher<>(consumer, null, null);
  }

  private static Set<TopicPartition> partitions(
      final String topic, final Collection<TopicPartition> partitions) {
    return partitions.stream().filter(p -> p.topic().equals(topic)).collect(toSet());
  }

  private boolean allTopicsCancelled() {
    return cancelled.equals(topics);
  }

  private Set<TopicPartition> assigned(final String topic) {
    return partitions(topic, consumer.assignment());
  }

  private void cancelTopic(final String topic) {
    cancelled.add(topic);

    if (allTopicsCancelled()) {
      stop = true;
    }
  }

  private void close(final KafkaConsumer<K, V> consumer) {
    sendEvent(ConsumerEvent.STOPPED);
    LOGGER.finest(() -> "Closing consumer " + consumer);
    consumer.close();
  }

  private TopicPublisher<K, V> createPublisher(final String topic) {
    return new TopicPublisher<>(topic, recordsToCommit::addLast, this::cancelTopic);
  }

  private Map<String, TopicPublisher<K, V>> createPublishers() {
    return ofNullable(topics).stream()
        .flatMap(Set::stream)
        .collect(toMap(t -> t, this::createPublisher));
  }

  private void commit() {
    getForever(
        () ->
            Optional.of(offsets(consumeHead(recordsToCommit)))
                .filter(offsets -> !offsets.isEmpty())
                .map(
                    offsets ->
                        SideEffect.<Boolean>run(
                                () ->
                                    getConsumer()
                                        .ifPresent(
                                            c -> {
                                              c.commitSync(trace("Commit", offsets));
                                              removePendingCommits(offsets);
                                            }))
                            .andThenGet(() -> true))
                .orElse(false));
  }

  private void dispatch(final ConsumerRecords<K, V> records) {
    publishers.forEach(
        (t, p) -> dispatch(t, p, stream(records.records(t).iterator()).collect(toList())));
  }

  private void dispatch(
      final String topic,
      final TopicPublisher<K, V> publisher,
      final List<ConsumerRecord<K, V>> records) {
    if (!records.isEmpty()) {
      pendingCommits.putAll(offsets(records.stream()));
      publisher.next(records);
    }

    if (!publisher.more()) {
      pause(topic);
    } else {
      resume(topic);
    }
  }

  private Optional<KafkaConsumer<K, V>> getConsumer() {
    if (consumer == null && consumerSupplier != null && topics != null) {
      consumer = consumerSupplier.get();
      consumer.subscribe(topics, new RebalanceListener());
    }

    return ofNullable(consumer);
  }

  private <T> T getForever(final Supplier<T> fn) {
    return tryToGetForever(() -> completedFuture(fn.get()), RETRY, this::panic)
        .toCompletableFuture()
        .join();
  }

  private Map<TopicPartition, OffsetAndMetadata> offsets(
      final Stream<ConsumerRecord<K, V>> records) {
    return records
        .map(
            r ->
                pair(
                    new TopicPartition(r.topic(), r.partition()),
                    new OffsetAndMetadata(r.offset() + 1)))
        .collect(
            toMap(
                pair -> pair.first,
                pair -> pair.second,
                (o1, o2) -> o1.offset() > o2.offset() ? o1 : o2));
  }

  private void panic(final Exception exception) {
    LOGGER.log(SEVERE, exception.getMessage(), exception);

    if (consumer != null) {
      LOGGER.finest(() -> "Closing consumer " + consumer);
      consumer.close();
      consumer = null;
    }
  }

  private void pause(final String topic) {
    if (!paused.contains(topic)) {
      paused.add(topic);
      LOGGER.log(FINE, () -> "Pause " + topic);
      consumer.pause(assigned(topic));
    }
  }

  private Set<TopicPartition> paused(final String topic) {
    return partitions(topic, consumer.paused());
  }

  private ConsumerRecords<K, V> poll() {
    return getForever(() -> getConsumer().map(c -> c.poll(POLL_TIMEOUT)).orElse(null));
  }

  /**
   * Returns the publisher for each topic, which is the key of the map.
   *
   * @return The map of publishers.
   */
  public Map<String, Publisher<ConsumerRecord<K, V>>> publishers() {
    return publishers.entrySet().stream().collect(toMap(Entry::getKey, Entry::getValue));
  }

  private void removePendingCommits(final Map<TopicPartition, OffsetAndMetadata> committed) {
    committed.forEach(
        (k, v) ->
            ofNullable(pendingCommits.get(k))
                .filter(o -> o.offset() <= v.offset())
                .ifPresent(o -> pendingCommits.remove(k)));
  }

  private void resume(final String topic) {
    if (paused.contains(topic)) {
      LOGGER.log(FINE, () -> "Resume " + topic);
      consumer.resume(paused(topic));
      paused.remove(topic);
    }
  }

  private void sendEvent(final ConsumerEvent event) {
    if (eventHandler != null) {
      getConsumer().ifPresent(c -> eventHandler.accept(event, c));
    }
  }

  /**
   * Starts the publisher. The method blocks until the publisher is stopped, which happens with
   * either a specific request of when all the topic streams have been cancelled.
   */
  public void start() {
    LOGGER.finest("Starting");

    if (consumerSupplier == null) {
      throw new IllegalArgumentException("Can't run without a consumer.");
    }

    if (topics == null || topics.isEmpty()) {
      throw new IllegalArgumentException("Can't run without topics.");
    }

    while (!stop) {
      commit();
      dispatch(poll());
    }

    LOGGER.finest("Stopped polling");

    getConsumer()
        .ifPresent(
            c -> {
              stopPublishers();
              waitForPendingCommits();
              close(c);
            });
  }

  /** Signals the request to stop the publisher, which will wind down in an orderly way. */
  public void stop() {
    LOGGER.finest("Stop requested");
    stop = true;
  }

  private void stopPublishers() {
    LOGGER.finest("Stopping publishers");

    publishers.entrySet().stream()
        .filter(e -> !cancelled.contains(e.getKey()))
        .forEach(
            e -> {
              pause(e.getKey());
              e.getValue().complete();
            });
  }

  private void waitForPendingCommits() {
    final State<Integer> waited = new State<>();

    waited.set(0);

    while (!allTopicsCancelled() && !pendingCommits.isEmpty() && waited.get() < WAIT_TIMEOUT) {
      tryToDo(
          () -> {
            commit();
            sleep(100);
            waited.set(waited.get() + 100);
          });
    }

    if (waited.get() >= WAIT_TIMEOUT) {
      LOGGER.info("Timeout pending commits.");
    }
  }

  /**
   * Creates a publisher with a Kafka consumer function. It may be called several times, for example
   * when there are issues with the Kafka cluster. You should not use the auto-commit feature in the
   * configuration of the consumer.
   *
   * @param consumer the function to generate a new Kafka consumer.
   * @return A new publisher instance.
   */
  public KafkaPublisher<K, V> withConsumer(final Supplier<KafkaConsumer<K, V>> consumer) {
    return new KafkaPublisher<>(consumer, topics, eventHandler);
  }

  /**
   * Creates a publisher with an event handler that receives lifecycle events from the publisher.
   *
   * @param eventHandler the function that consumes the events. It may be <code>null</code>.
   * @return A new publisher instance.
   */
  public KafkaPublisher<K, V> withEventHandler(
      final BiConsumer<ConsumerEvent, KafkaConsumer<K, V>> eventHandler) {
    return new KafkaPublisher<>(consumerSupplier, topics, eventHandler);
  }

  /**
   * Creates a publisher with a set of topics that should be consumed.
   *
   * @param topics the topic names.
   * @return A new publisher instance.
   */
  public KafkaPublisher<K, V> withTopics(final Set<String> topics) {
    return new KafkaPublisher<>(consumerSupplier, topics, eventHandler);
  }

  private class RebalanceListener implements ConsumerRebalanceListener {
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
      if (!started) {
        started = true;
        sendEvent(ConsumerEvent.STARTED);
      }
    }

    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
      // Not interested.
    }
  }
}
