package net.pincette.rs.kafka;

import static com.mongodb.client.model.Filters.eq;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static net.pincette.mongo.Collection.findOne;
import static net.pincette.mongo.Collection.replaceOne;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.generate;
import static net.pincette.rs.Util.onComplete;
import static net.pincette.rs.kafka.TestUtil.COMMON_CONFIG;
import static net.pincette.rs.kafka.TestUtil.checkOffsets;
import static net.pincette.rs.kafka.TestUtil.measure;
import static net.pincette.rs.kafka.TestUtil.newTopic;
import static net.pincette.rs.kafka.TestUtil.offsets;
import static net.pincette.rs.kafka.TestUtil.producerEventHandler;
import static net.pincette.rs.kafka.TestUtil.testComplete;
import static net.pincette.rs.kafka.TestUtil.topic;
import static net.pincette.rs.kafka.Util.createTopics;
import static net.pincette.rs.kafka.Util.deleteTopics;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.apache.kafka.clients.admin.Admin.create;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import net.pincette.rs.Async;
import net.pincette.rs.Mapper;
import net.pincette.rs.Merge;
import net.pincette.rs.PassThrough;
import net.pincette.util.Pair;
import net.pincette.util.State;
import net.pincette.util.Util.GeneralException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestKafka {
  private static final MongoCollection<BsonDocument> collection =
      MongoClients.create("mongodb://localhost:27017")
          .getDatabase("test")
          .getCollection("test", BsonDocument.class);
  private final String inTopic1 = topic();
  private final String inTopic2 = topic();
  private final String outTopic1 = topic();
  private final String outTopic2 = topic();
  private final Set<String> topics = set(inTopic1, inTopic2, outTopic1, outTopic2);

  private static <T> Function<T, T> checkOrder(
      final int start, final Function<T, String> value, final String message) {
    final State<Integer> state = new State<>(start);

    return rec -> {
      final int v = parseInt(value.apply(rec));

      if (state.get() != MAX_VALUE) {
        assertEquals(state.get() + 1, v, message);
      }

      state.set(v);

      return rec;
    };
  }

  private static Function<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
      checkOrderConsumer(final int start, final String message) {
    return checkOrder(start, ConsumerRecord::value, message);
  }

  private static Function<ProducerRecord<String, String>, ProducerRecord<String, String>>
      checkOrderProducer(final int start, final String message) {
    return checkOrder(start, ProducerRecord::value, message);
  }

  private static Function<ProducerRecord<String, String>, ProducerRecord<String, String>>
      checkOrderProducer(final String topic, final int start, final String message) {
    final Function<ProducerRecord<String, String>, ProducerRecord<String, String>> check =
        checkOrder(start, ProducerRecord::value, message);

    return rec -> topic.equals(rec.topic()) ? check.apply(rec) : rec;
  }

  private static Publisher<Integer> flat(
      final KafkaPublisher<String, String> publisher,
      final String topic,
      final int max,
      final boolean resume) {
    return with(publisher.publishers().get(topic))
        .map(checkOrderConsumer(resume ? MAX_VALUE : 0, "flat at " + topic))
        .map(rec -> parseInt(rec.value()))
        .until(v -> v == max)
        .get();
  }

  private static Publisher<ProducerRecord<String, String>> generator(
      final int max, final String topic) {
    return with(generate(() -> 0, v -> v < max - 1 ? (v + 1) : null))
        .map(v -> new ProducerRecord<>(topic, valueOf(v), valueOf(v)))
        .map(checkOrderProducer(-1, "generator"))
        .get();
  }

  private static Publisher<ProducerRecord<String, String>> processor(
      final KafkaPublisher<String, String> publisher,
      final String inTopic,
      final String outTopic,
      final Processor<Pair<String, Integer>, Pair<String, Integer>> incrementer,
      final boolean resume) {
    return with(publisher.publishers().get(inTopic))
        .map(checkOrderConsumer(resume ? MAX_VALUE : -1, "processor at input topic " + inTopic))
        .map(r -> pair(r.key(), parseInt(r.value())))
        .map(incrementer)
        .map(pair -> new ProducerRecord<>(outTopic, pair.first, valueOf(pair.second)))
        .map(checkOrderProducer(resume ? MAX_VALUE : 0, "processor at output topic " + outTopic))
        .get();
  }

  private static Function<Integer, Integer> tester(final boolean resume) {
    final State<Integer> state = new State<>(resume ? MAX_VALUE : 0);

    return value -> {
      if (state.get() != MAX_VALUE) {
        assertEquals(state.get() + 1, value);
      }

      state.set(value);

      return value;
    };
  }

  @AfterEach
  public void after() {
    tryToDoWithRethrow(
        () -> create(COMMON_CONFIG),
        admin -> deleteTopics(topics, admin).toCompletableFuture().join());
  }

  @Test
  @DisplayName("async")
  void async() {
    final int max = 200000;

    runTest(
        max,
        () ->
            box(
                new Mapper<>(pair -> supplyAsync(() -> pair(pair.first, pair.second + 1))),
                new Async<>()),
        false,
        (start, end) -> end == start + max);
  }

  @BeforeEach
  public void before() {
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
  @DisplayName("exception async")
  void exceptionAsync() {
    runTest(
        3,
        () ->
            pipe(new Mapper<Pair<String, Integer>, CompletionStage<Pair<String, Integer>>>(
                    pair ->
                        supplyAsync(
                            () -> {
                              if (pair.second == 1) {
                                throw new GeneralException("test");
                              }

                              return pair(pair.first, pair.second + 1);
                            })))
                .then(new Async<>()),
        true,
        (start, end) -> end == -1);
  }

  @Test
  @DisplayName("exception directly")
  void exceptionDirectly() {
    runTest(
        3,
        () ->
            new Mapper<>(
                pair -> {
                  throw new GeneralException("test");
                }),
        true,
        (start, end) -> end == -1);
  }

  @Test
  @DisplayName("exception middle")
  void exceptionMiddle() {
    runTest(
        1000,
        () ->
            new Mapper<>(
                pair -> {
                  if (pair.second == 345) {
                    throw new GeneralException("test");
                  }

                  return pair(pair.first, pair.second + 1);
                }),
        true,
        (start, end) -> end <= 345);
  }

  private void fill(final int max, final String topic) {
    final KafkaSubscriber<String, String> subscriber =
        new KafkaSubscriber<String, String>().withProducer(TestUtil::producer);

    generator(max, topic).subscribe(subscriber);
    subscriber.join();
  }

  @Test
  @DisplayName("mongo")
  void mongo() {
    final int max = 20000;

    runTest(
        max,
        () ->
            box(
                new Mapper<>(
                    pair ->
                        replaceOne(
                                collection,
                                eq("_id", pair.first),
                                new BsonDocument(
                                    list(
                                        new BsonElement("_id", new BsonString(pair.first)),
                                        new BsonElement("value", new BsonInt32(pair.second + 1)))),
                                new ReplaceOptions().upsert(true))
                            .thenApply(result -> must(result, UpdateResult::wasAcknowledged))
                            .thenComposeAsync(
                                result ->
                                    findOne(
                                        collection,
                                        eq("_id", pair.first),
                                        BsonDocument.class,
                                        null))
                            .thenApply(result -> must(result, Optional::isPresent))
                            .thenApply(Optional::get)
                            .thenApply(
                                result -> pair(pair.first, result.getInt32("value").getValue()))),
                new Async<>()),
        false,
        (start, end) -> end == start + max);
  }

  @Test
  @DisplayName("plain")
  void plain() {
    final int max = 200000;

    runTest(
        max,
        () -> new Mapper<>(pair -> pair(pair.first, pair.second + 1)),
        false,
        (start, end) -> end == start + max);
  }

  @Test
  @DisplayName("resume")
  void resume() {
    final int max = 10000;

    fill(max, inTopic1);

    runProcessor(
        max,
        inTopic1,
        outTopic1,
        () ->
            new Mapper<>(
                pair -> {
                  if (pair.second == 4530) {
                    throw new GeneralException("test");
                  }

                  return pair(pair.first, pair.second + 1);
                }),
        (start, end) -> end <= 4530,
        false);

    runProcessor(
        max,
        inTopic1,
        outTopic1,
        () -> new Mapper<>(pair -> pair(pair.first, pair.second + 1)),
        (start, end) -> end == max,
        true);
  }

  private void runProcessor(
      final int max,
      final String inTopic,
      final String outTopic,
      final Supplier<Processor<Pair<String, Integer>, Pair<String, Integer>>> incrementer,
      final BiPredicate<Long, Long> offsetTest,
      final boolean resume) {
    final PassThrough<Integer> pass = new PassThrough<>();
    final KafkaSubscriber<String, String> subscriber =
        new KafkaSubscriber<String, String>()
            .withProducer(TestUtil::producer)
            .withEventHandler(
                (e, p) -> {
                  if (e == ProducerEvent.ERROR) {
                    pass.cancel();
                  }
                });
    final State<Map<TopicPartition, Long>> startOffsets = new State<>();
    final KafkaPublisher<String, String> publisher =
        new KafkaPublisher<String, String>()
            .withConsumer(TestUtil::consumer)
            .withEventHandler(
                (e, c) -> {
                  if (e == ConsumerEvent.STARTED) {
                    if (!resume) {
                      c.seekToBeginning(c.assignment());
                    }

                    startOffsets.set(offsets(c, set(inTopic)));
                  } else if (e == ConsumerEvent.STOPPED) {
                    assertTrue(
                        checkOffsets(startOffsets.get(), offsets(c, set(inTopic)), offsetTest));
                  }
                })
            .withTopics(set(inTopic, outTopic));

    processor(publisher, inTopic, outTopic, incrementer.get(), resume).subscribe(subscriber);
    with(flat(publisher, outTopic, max, resume))
        .map(tester(resume))
        .map(pass)
        .get()
        .subscribe(onComplete(publisher::stop));
    publisher.start();
  }

  private void runTest(
      final int max,
      final Supplier<Processor<Pair<String, Integer>, Pair<String, Integer>>> incrementer,
      final boolean cancel,
      final BiPredicate<Long, Long> offsetTest) {
    final State<Boolean> cancelled = new State<>(false);
    final PassThrough<Integer> pass1 = new PassThrough<>();
    final PassThrough<Integer> pass2 = new PassThrough<>();
    final State<Map<TopicPartition, Long>> startOffsets = new State<>();
    final KafkaSubscriber<String, String> subscriber =
        new KafkaSubscriber<String, String>()
            .withProducer(TestUtil::producer)
            .withEventHandler(
                (e, p) -> {
                  if (e == ProducerEvent.ERROR) {
                    cancelled.set(true);
                    pass1.cancel();
                    pass2.cancel();
                  }
                });
    final KafkaPublisher<String, String> publisher =
        new KafkaPublisher<String, String>()
            .withConsumer(TestUtil::consumer)
            .withEventHandler(producerEventHandler(startOffsets, topics, offsetTest))
            .withTopics(topics);

    with(Merge.of(
            processor(publisher, inTopic1, outTopic1, incrementer.get(), false),
            processor(publisher, inTopic2, outTopic2, incrementer.get(), false),
            generator(max, inTopic1),
            generator(max, inTopic2)))
        .buffer(1000)
        .map(checkOrderProducer(inTopic1, -1, "merge at " + inTopic1))
        .map(checkOrderProducer(inTopic2, -1, "merge at " + inTopic2))
        .map(checkOrderProducer(outTopic1, 0, "merge at " + outTopic1))
        .map(checkOrderProducer(outTopic2, 0, "merge at " + outTopic2))
        .get()
        .subscribe(subscriber);

    final AtomicInteger stopped = new AtomicInteger(2);

    with(flat(publisher, outTopic1, max, false))
        .map(tester(false))
        .map(pass1)
        .get()
        .subscribe(testComplete(publisher::stop, stopped));
    with(flat(publisher, outTopic2, max, false))
        .map(tester(false))
        .map(pass2)
        .get()
        .subscribe(testComplete(publisher::stop, stopped));

    measure(publisher::start);
    assertTrue((!cancel && !cancelled.get()) || (cancel && cancelled.get()));
  }
}
