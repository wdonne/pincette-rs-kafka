package net.pincette.rs.kafka;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
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
import static net.pincette.util.Util.tryToDoSilent;
import static net.pincette.util.Util.tryToGetForever;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import net.pincette.util.Pair;
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
 * @author Werner Donné
 */
public class KafkaPublisher<K, V> {
  private static final Duration POLL_TIMEOUT = ofMillis(100);
  private static final Duration RETRY = ofSeconds(5);
  private static final int WAIT_TIMEOUT = 3000;

  private final Queue<Pair<String, Boolean>> backpressure = new ConcurrentLinkedQueue<>();
  private final Supplier<KafkaConsumer<K, V>> consumerSupplier;
  private final BiConsumer<ConsumerEvent, KafkaConsumer<K, V>> eventHandler;
  private final Map<TopicPartition, OffsetAndMetadata> pendingCommits = new ConcurrentHashMap<>();
  private final Map<String, TopicPublisher<K, V>> publishers;
  private final Deque<ConsumerRecord<K, V>> recordsToCommit = new ConcurrentLinkedDeque<>();
  private final Set<String> topics;
  private final Duration throttleTime;
  private KafkaConsumer<K, V> consumer;
  private boolean started;
  private boolean stop;

  /**
   * Creates an uninitialized publisher. Use the <code>with</code> methods to produce initialized
   * instances.
   */
  public KafkaPublisher() {
    this(null, null, null, null);
  }

  private KafkaPublisher(
      final Supplier<KafkaConsumer<K, V>> consumer,
      final Set<String> topics,
      final BiConsumer<ConsumerEvent, KafkaConsumer<K, V>> eventHandler,
      final Duration throttleTime) {
    this.consumerSupplier = consumer;
    this.topics = topics;
    this.eventHandler = eventHandler;
    this.throttleTime = throttleTime;
    publishers = createPublishers();
  }

  public static <K, V> KafkaPublisher<K, V> publisher(
      final Supplier<KafkaConsumer<K, V>> consumer) {
    return new KafkaPublisher<>(consumer, null, null, null);
  }

  private static Set<TopicPartition> partitions(
      final String topic, final Collection<TopicPartition> partitions) {
    return partitions.stream().filter(p -> p.topic().equals(topic)).collect(toSet());
  }

  private boolean allTopicsCancelled() {
    return topics.stream()
        .map(publishers::get)
        .filter(Objects::nonNull)
        .allMatch(TopicPublisher::cancelled);
  }

  private Set<TopicPartition> assignedPartitions(final String topic) {
    return partitions(topic, consumer.assignment());
  }

  private void close(final KafkaConsumer<K, V> consumer) {
    sendEvent(ConsumerEvent.STOPPED);
    trace(() -> "Closing consumer " + consumer);
    consumer.close();
  }

  private List<Pair<String, Boolean>> collectBackpressure() {
    final List<Pair<String, Boolean>> result = new ArrayList<>(backpressure.size());

    while (!backpressure.isEmpty()) {
      result.add(backpressure.remove());
    }

    return result;
  }

  private void commit() {
    Optional.of(offsets(consumeHead(recordsToCommit)))
        .filter(offsets -> !offsets.isEmpty())
        .ifPresent(this::commitOffsets);
  }

  private void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    getConsumer()
        .ifPresent(
            c -> {
              c.commitSync(trace("Commit", offsets));
              removePendingCommits(offsets);
            });
  }

  private TopicPublisher<K, V> createPublisher(final String topic) {
    return new TopicPublisher<>(topic, recordsToCommit::addLast, backpressure);
  }

  private Map<String, TopicPublisher<K, V>> createPublishers() {
    return ofNullable(topics).stream()
        .flatMap(Set::stream)
        .collect(toMap(t -> t, this::createPublisher));
  }

  private void dispatch(final ConsumerRecords<K, V> records) {
    publishers.forEach(
        (t, p) -> dispatch(p, stream(records.records(t).iterator()).collect(toList())));
  }

  private void dispatch(
      final TopicPublisher<K, V> publisher, final List<ConsumerRecord<K, V>> records) {
    if (!records.isEmpty()) {
      pendingCommits.putAll(offsets(records.stream()));
      publisher.next(records);
    }
  }

  private int excessQueuedBatches() {
    return queuedBatches() - publishers.size();
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

  private void holdYourHorses() {
    shouldWait().ifPresent(millis -> tryToDoSilent(() -> sleep(millis)));
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
      trace(() -> "Closing consumer " + consumer);
      consumer.close();
      consumer = null;
    }
  }

  private void pause(final String topic) {
    getConsumer()
        .ifPresent(
            c -> {
              LOGGER.fine(() -> "Pause " + topic);
              c.pause(assignedPartitions(topic));
            });
  }

  private void pauseAll() {
    getConsumer().ifPresent(c -> topics.forEach(this::pause));
  }

  private void pauseOrResumeTopics() {
    processBackpressure()
        .forEach(
            (topic, resume) -> {
              if (Boolean.TRUE.equals(resume)) {
                resume(topic);
              } else {
                pause(topic);
              }
            });
  }

  private Set<TopicPartition> pausedPartitions(final String topic) {
    return partitions(topic, consumer.paused());
  }

  private ConsumerRecords<K, V> poll() {
    return getForever(() -> getConsumer().map(c -> c.poll(POLL_TIMEOUT)).orElse(null));
  }

  private Map<String, Boolean> processBackpressure() {
    return collectBackpressure().stream()
        .collect(toMap(p -> p.first, p -> p.second, (v1, v2) -> v2));
  }

  /**
   * Returns the publisher for each topic, which is the key of the map.
   *
   * @return The map of publishers.
   */
  public Map<String, Publisher<ConsumerRecord<K, V>>> publishers() {
    return publishers.entrySet().stream().collect(toMap(Entry::getKey, Entry::getValue));
  }

  private int queuedBatches() {
    return publishers.values().stream().mapToInt(TopicPublisher::queued).sum();
  }

  private void removePendingCommits(final Map<TopicPartition, OffsetAndMetadata> committed) {
    committed.forEach(
        (k, v) ->
            ofNullable(pendingCommits.get(k))
                .filter(o -> o.offset() <= v.offset())
                .ifPresent(o -> pendingCommits.remove(k)));
  }

  private void resume(final String topic) {
    getConsumer()
        .ifPresent(
            c -> {
              LOGGER.fine(() -> "Resume " + topic);
              c.resume(pausedPartitions(topic));
            });
  }

  private void sendEvent(final ConsumerEvent event) {
    if (eventHandler != null) {
      getConsumer().ifPresent(c -> eventHandler.accept(event, c));
    }
  }

  private Optional<Long> shouldWait() {
    return ofNullable(throttleTime)
        .map(time -> pair(time, excessQueuedBatches()))
        .filter(pair -> pair.second > 0)
        .map(pair -> pair.first)
        .map(Duration::toMillis);
  }

  /**
   * Starts the publisher. The method blocks until the publisher is stopped, which happens with
   * either a specific request of when all the topic streams have been cancelled.
   */
  public void start() {
    trace("Starting");

    if (consumerSupplier == null) {
      throw new IllegalArgumentException("Can't run without a consumer.");
    }

    if (topics == null || topics.isEmpty()) {
      throw new IllegalArgumentException("Can't run without topics.");
    }

    pauseAll();

    while (!stop) {
      commit();
      pauseOrResumeTopics();

      if (allTopicsCancelled()) {
        stop = true;
      } else {
        dispatch(poll());
        holdYourHorses();
      }
    }

    trace("Stopped polling");
    pauseAll();

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
    trace("Stop requested");
    stop = true;
  }

  private void stopPublishers() {
    trace("Stopping publishers");
    publishers.values().stream().filter(p -> !p.cancelled()).forEach(TopicPublisher::complete);
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
    return new KafkaPublisher<>(consumer, topics, eventHandler, throttleTime);
  }

  /**
   * Creates a publisher with an event handler that receives lifecycle events from the publisher.
   *
   * @param eventHandler the function that consumes the events. It may be <code>null</code>.
   * @return A new publisher instance.
   */
  public KafkaPublisher<K, V> withEventHandler(
      final BiConsumer<ConsumerEvent, KafkaConsumer<K, V>> eventHandler) {
    return new KafkaPublisher<>(consumerSupplier, topics, eventHandler, throttleTime);
  }

  /**
   * Creates a publisher with a set of topics that should be consumed.
   *
   * @param topics the topic names.
   * @return A new publisher instance.
   */
  public KafkaPublisher<K, V> withTopics(final Set<String> topics) {
    return new KafkaPublisher<>(consumerSupplier, topics, eventHandler, throttleTime);
  }

  /**
   * Creates a publisher which throttles the poll loop when more batches have been queued than there
   * are topic publishers.
   *
   * @param throttleTime the time the poll loop is stalled.
   * @return A new publisher instance.
   */
  public KafkaPublisher<K, V> withThrottleTime(final Duration throttleTime) {
    return new KafkaPublisher<>(consumerSupplier, topics, eventHandler, throttleTime);
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
