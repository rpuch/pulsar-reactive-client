package com.rpuch.pulsar.reactive.it;

import com.rpuch.pulsar.reactive.api.ReactivePulsarClient;
import com.rpuch.pulsar.reactive.api.ReactiveReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * @author Roman Puchkovskiy
 */
public class IntegrationTests extends TestWithPulsar {
    private PulsarClient coreClient;
    private ReactivePulsarClient reactiveClient;

    private final String topic = UUID.randomUUID().toString();

    @BeforeEach
    void createClients() throws Exception {
        coreClient = PulsarClient.builder()
                .serviceUrl(pulsarBrokerUrl())
                .build();
        reactiveClient = ReactivePulsarClient.from(coreClient);
    }

    @AfterEach
    void cleanUp() throws Exception {
        coreClient.close();
    }

    @Test
    void receiveReadsSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Flux<Message<byte[]>> messages = reactiveClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .receive();
        List<Integer> resultInts = messages.map(Message::getData)
                .map(this::intFromBytes)
                .take(10)
                .toStream().collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    private void produceZeroToNineWithoutSchema() throws PulsarClientException {
        try (Producer<byte[]> producer = coreClient.newProducer().topic(topic).create()) {
            for (int i = 0; i < 10; i++) {
                producer.send(intToBytes(i));
            }
        }
    }

    private byte[] intToBytes(int n) {
        String str = Integer.toString(n);
        return str.getBytes(UTF_8);
    }

    private int intFromBytes(byte[] bytes) {
        String str = new String(bytes, UTF_8);
        return Integer.parseInt(str);
    }

    private List<Integer> listOfZeroToNine() {
        return IntStream.range(0, 10).boxed().collect(toList());
    }

    @Test
    void receiveStartedWithMessageIdLatestBeforeDataIsProducedReadsTheDataAsItIsProduced() throws Exception {
        ConnectableFlux<Message<byte[]>> messages = reactiveClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.latest)
                .receive()
                .replay();
        messages.connect();

        produceZeroToNineWithoutSchema();

        List<Integer> resultInts = messages.map(Message::getData)
                .map(this::intFromBytes)
                .take(10)
                .timeout(Duration.ofSeconds(10))
                .toStream()
                .collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    @Test
    void receiveReadsSuccessfullyWithSchema() throws Exception {
        produceZeroToNineWithStringSchema();

        Flux<Message<String>> messages = reactiveClient.newReader(StringSchema.utf8())
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .receive();
        List<Integer> resultInts = messages.map(Message::getValue)
                .map(Integer::valueOf)
                .take(10)
                .toStream().collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    private void produceZeroToNineWithStringSchema() throws PulsarClientException {
        try (Producer<String> producer = coreClient.newProducer(StringSchema.utf8()).topic(topic).create()) {
            for (int i = 0; i < 10; i++) {
                producer.send(Integer.toString(i));
            }
        }
    }

    @Test
    void forOneReadsSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Mono<Message<byte[]>> messageMono = reactiveClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .forOne(reader -> reader.receive().next());

        StepVerifier.create(messageMono)
                .assertNext(message -> assertThat(intFromBytes(message.getData()), is(0)));
    }

    @Test
    void forManyReadsSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Flux<Message<byte[]>> messages = reactiveClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .forMany(ReactiveReader::receive);
        List<Integer> resultInts = messages.map(Message::getValue)
                .map(this::intFromBytes)
                .take(10)
                .toStream().collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    @Test
    @Disabled("Enable back when it becomes clear to 'take data until the topic is terminated'")
    void hasReachedEndOfTopicSeesEndOfTopic() throws Exception {
        produceZeroToNineWithoutSchema();
        terminateTopic();

        Flux<Message<byte[]>> messages = reactiveClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .forMany(
                        reader -> reader.receive().takeUntil(msg -> reader.hasReachedEndOfTopic())
                );
        List<Integer> resultInts = messages.map(Message::getValue)
                .map(this::intFromBytes)
                .timeout(Duration.ofSeconds(10))
                .toStream().collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    private void terminateTopic() {
        new AdminClient(adminServiceUrl()).terminateTopic(topic);
    }

    @Test
    void hasMessageAvailableReturnsExpectedValues() throws Exception {
        produceZeroToNineWithoutSchema();

        Mono<Tuple2<Boolean, Boolean>> trueFalse = reactiveClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .forOne(
                        reader -> reader.hasMessageAvailable().zipWhen(
                                availableBefore -> reader.receive().take(10).then(reader.hasMessageAvailable())
                        )
                );

        StepVerifier.create(trueFalse)
                .expectNext(Tuples.of(true, false))
                .verifyComplete();
    }

    @Test
    void seekByMessageIdPositionsSubsequentReads() throws Exception {
        produceZeroToNineWithoutSchema();
        MessageId thirdMessageId = getThirdMessageId();

        Message<byte[]> fourthMessage = reactiveClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .forMany(reader -> reader.seek(thirdMessageId).thenMany(reader.receive()))
                .blockFirst();
        assertThat(fourthMessage, notNullValue());

        assertThat(intFromBytes(fourthMessage.getData()), is(3));
    }

    private MessageId getThirdMessageId() {
        Message<byte[]> thirdMessage = reactiveClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .forMany(reactiveReader -> reactiveReader.receive().take(3))
                .blockLast();
        assertThat(thirdMessage, notNullValue());
        return thirdMessage.getMessageId();
    }

    @Test
    void returnsPartitionsViaGetPartitionsForTopic() throws Exception {
        triggerTopicCreation();

        reactiveClient.getPartitionsForTopic(topic)
                .as(StepVerifier::create)
                .expectNext(singletonList(topic))
                .verifyComplete();
    }

    private void triggerTopicCreation() throws PulsarClientException {
        produceZeroToNineWithoutSchema();
    }
}
