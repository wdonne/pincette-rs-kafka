package net.pincette.rs.kafka;

import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.generate;
import static net.pincette.rs.kafka.KafkaPublisher.publisher;
import static net.pincette.rs.kafka.KafkaSubscriber.subscriber;
import static net.pincette.rs.kafka.TestUtil.COMMON_CONFIG;
import static net.pincette.rs.kafka.TestUtil.measure;
import static net.pincette.rs.kafka.TestUtil.newTopic;
import static net.pincette.rs.kafka.TestUtil.consumerEventHandler;
import static net.pincette.rs.kafka.TestUtil.testComplete;
import static net.pincette.rs.kafka.TestUtil.topic;
import static net.pincette.rs.kafka.Util.createTopics;
import static net.pincette.rs.kafka.Util.deleteTopics;
import static net.pincette.rs.kafka.Util.fromPublisher;
import static net.pincette.rs.kafka.Util.fromSubscriber;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.apache.kafka.clients.admin.Admin.create;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.Streams;
import net.pincette.rs.streams.TopicSource;
import net.pincette.util.State;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestStreams {
  private final String inTopic1 = topic("inTopic1");
  private final String inTopic2 = topic("inTopic2");
  private final String outTopic1 = topic("outTopic1");
  private final String outTopic2 = topic("outTopic2");
  private final Set<String> topics = set(inTopic1, inTopic2, outTopic1, outTopic2);

  private static Publisher<Message<String, String>> generator(final int max) {
    return with(generate(() -> 0, v -> v < max - 1 ? (v + 1) : null))
        .map(v -> message(valueOf(v), valueOf(v)))
        .get();
  }

  private static Publisher<Message<String, String>> processor(
      final Publisher<Message<String, String>> publisher) {
    return with(publisher).map(m -> message(m.key, valueOf(parseInt(m.value) + 1))).get();
  }

  private static Publisher<Integer> test(
      final Publisher<Message<String, String>> publisher, final int max) {
    return with(publisher).map(m -> parseInt(m.value)).until(v -> v == max).map(tester()).get();
  }

  private static Function<Integer, Integer> tester() {
    final State<Integer> state = new State<>(0);

    return value -> {
      assertEquals(state.get() + 1, value);
      state.set(value);

      return value;
    };
  }

  @AfterEach
  void after() {
    tryToDoWithRethrow(
        () -> create(COMMON_CONFIG),
        admin -> deleteTopics(topics, admin).toCompletableFuture().join());
  }

  @BeforeEach
  void before() {
    tryToDoWithRethrow(
        () -> create(COMMON_CONFIG),
        admin ->
            createTopics(
                    set(
                        newTopic(inTopic1),
                        newTopic(inTopic2),
                        newTopic(outTopic1),
                        newTopic(outTopic2)),
                    admin)
                .toCompletableFuture()
                .join());
  }

  @Test
  @DisplayName("streams")
  void streams() {
    final int max = 200000;
    final State<TopicSource<String, String, ConsumerRecord<String, String>>> source = new State<>();
    final State<Map<TopicPartition, Long>> startOffsets = new State<>();
    final Function<Set<String>, TopicSource<String, String, ConsumerRecord<String, String>>> fn =
        topics -> {
          source.set(
              fromPublisher(
                      publisher(TestUtil::consumer)
                          .withEventHandler(
                              consumerEventHandler(
                                  startOffsets, topics, (start, end) -> end == start + max)))
                  .apply(topics));
          return source.get();
        };
    final AtomicInteger stopped = new AtomicInteger(2);

    measure(
        () ->
            Streams.streams(fn, fromSubscriber(subscriber(TestUtil::producer)))
                .to(inTopic1, generator(max))
                .to(inTopic2, generator(max))
                .from(inTopic1, TestStreams::processor)
                .to(outTopic1)
                .from(inTopic2, TestStreams::processor)
                .to(outTopic2)
                .consume(
                    outTopic1,
                    p -> test(p, max).subscribe(testComplete(() -> source.get().stop(), stopped)))
                .consume(
                    outTopic2,
                    p -> test(p, max).subscribe(testComplete(() -> source.get().stop(), stopped)))
                .start());
  }
}
