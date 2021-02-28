/*
 * Copyright 2021 Pulsar Reactive Client contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rpuch.pulsar.reactor.it;

import com.rpuch.pulsar.reactor.api.ReactiveConsumer;
import com.rpuch.pulsar.reactor.api.ReactivePulsarClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest;
import static org.apache.pulsar.client.api.SubscriptionInitialPosition.Latest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * @author Roman Puchkovskiy
 */
public class ConsumerIntegrationTests extends TestWithPulsar {
    private PulsarClient coreClient;
    private ReactivePulsarClient reactiveClient;

    private final MessageConverter converter = new MessageConverter();

    private final String topic = randomString();
    private final String subscriptionName = randomSubscriptionName();

    @BeforeEach
    void createClients() throws Exception {
        coreClient = PulsarClient.builder()
                .serviceUrl(pulsarBrokerUrl())
                .build();
        reactiveClient = ReactivePulsarClient.from(coreClient);
    }

    @AfterEach
    void cleanUp() throws Exception {
        reactiveClient.close();
    }

    @Test
    void messagesOnConsumerReadsSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Flux<Message<byte[]>> messages = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forMany(ReactiveConsumer::messages);
        List<Integer> resultInts = messages.map(Message::getData)
                .map(this::intFromBytes)
                .take(10)
                .toStream().collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    private void produceZeroToNineWithoutSchema() throws PulsarClientException {
        boolean batchingEnabled = true;
        produceZeroToNineWithoutSchema(batchingEnabled);
    }

    private List<MessageId> produceZeroToNineWithoutSchema(boolean batchingEnabled) throws PulsarClientException {
        List<MessageId> messageIds = new ArrayList<>();

        try (Producer<byte[]> producer = coreClient.newProducer().topic(topic).enableBatching(batchingEnabled).create()) {
            for (int i = 0; i < 10; i++) {
                MessageId messageId = producer.send(intToBytes(i));
                messageIds.add(messageId);
            }
        }

        return messageIds;
    }

    private byte[] intToBytes(int n) {
        return converter.intToBytes(n);
    }

    private int intFromBytes(byte[] bytes) {
        return converter.intFromBytes(bytes);
    }

    private List<Integer> listOfZeroToNine() {
        return IntStream.range(0, 10).boxed().collect(toList());
    }

    @Test
    @Disabled("Enable when it's clear why nothing is seen")
    void messagesOnConsumerStartedWithSubscriptionInitialPositionLatestBeforeDataIsProducedReadsTheDataAsItIsProduced()
            throws Exception {
        ConnectableFlux<Message<byte[]>> messages = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Latest)
                .subscriptionName(randomSubscriptionName())
                .forMany(ReactiveConsumer::messages)
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
    void messagesOnConsumerReadsSuccessfullyWithSchema() throws Exception {
        produceZeroToNineWithStringSchema();

        Flux<Message<String>> messages = reactiveClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forMany(ReactiveConsumer::messages);
        List<Integer> resultInts = messages.map(Message::getValue)
                .map(converter::intFromString)
                .take(10)
                .toStream().collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    private void produceZeroToNineWithStringSchema() throws PulsarClientException {
        try (Producer<String> producer = coreClient.newProducer(Schema.STRING).topic(topic).create()) {
            for (int i = 0; i < 10; i++) {
                producer.send(converter.intToString(i));
            }
        }
    }

    @Test
    void receiveOnConsumerReadsSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Mono<Message<byte[]>> firstMessage = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forOne(ReactiveConsumer::receive);

        StepVerifier.create(firstMessage)
                .assertNext(message -> assertThat(intFromBytes(message.getData()), is(0)))
                .verifyComplete();
    }

    @Test
    void batchReceiveOnConsumerReadsSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Mono<Messages<byte[]>> messagesMono = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forOne(ReactiveConsumer::batchReceive);

        List<Integer> resultInts = messagesMono.flatMapIterable(this::messageListFromMessages)
                .map(Message::getValue)
                .map(converter::intFromBytes)
                .take(10)
                .timeout(Duration.ofSeconds(1))
                .toStream().collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    private <T> List<Message<T>> messageListFromMessages(Messages<T> messages) {
        return StreamSupport.stream(messages.spliterator(), false)
                .collect(toList());
    }

    @Test
    void forOneReadsSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Mono<Message<byte[]>> messageMono = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forOne(consumer -> consumer.messages().next());

        StepVerifier.create(messageMono)
                .assertNext(message -> assertThat(intFromBytes(message.getData()), is(0)));
    }

    @Test
    void forManyReadsSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Flux<Message<byte[]>> messages = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forMany(ReactiveConsumer::messages);
        List<Integer> resultInts = messages.map(Message::getValue)
                .map(this::intFromBytes)
                .take(10)
                .toStream().collect(toList());

        assertThat(resultInts, equalTo(listOfZeroToNine()));
    }

    @Test
    void unsubscribeOnConsumerUnsubscribesSuccessfully() throws Exception {
        produceZeroToNineWithoutSchema();

        Mono<Void> mono = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forOne(consumer -> consumer.receive().then(consumer.unsubscribe()));

        StepVerifier.create(mono)
                .verifyComplete();
    }

    @Test
    @Disabled("Enable back when it becomes clear how to 'take data until the topic is terminated'")
    void hasReachedEndOfTopicSeesEndOfTopic() throws Exception {
        produceZeroToNineWithoutSchema();
        terminateTopic();

        Flux<Message<byte[]>> messages = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forMany(
                        consumer -> consumer.messages().takeUntil(msg -> consumer.hasReachedEndOfTopic())
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
    void seekByMessageIdPositionsSubsequentReads() throws Exception {
        produceZeroToNineWithoutSchema();
        MessageId thirdMessageId = getThirdMessage().getMessageId();

        Message<byte[]> fourthMessage = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forMany(consumer -> consumer.seek(thirdMessageId).thenMany(consumer.messages()))
                .blockFirst();
        assertThat(fourthMessage, notNullValue());

        assertThat(intFromBytes(fourthMessage.getData()), is(3));
    }

    private Message<byte[]> getThirdMessage() {
        Message<byte[]> thirdMessage = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forMany(consumer -> consumer.messages().take(3))
                .blockLast();
        assertThat(thirdMessage, notNullValue());
        return thirdMessage;
    }

    private String randomSubscriptionName() {
        return randomString();
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }

    @Test
    void seekByTimestampPositionsSubsequentReads() throws Exception {
        produceZeroToNineWithoutSchema();
        long thirdMessageTimestamp = getThirdMessage().getPublishTime();

        Message<byte[]> thirdMessageMessageAgain = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forMany(consumer -> consumer.seek(thirdMessageTimestamp).thenMany(consumer.messages()))
                .blockFirst();
        assertThat(thirdMessageMessageAgain, notNullValue());

        assertThat(intFromBytes(thirdMessageMessageAgain.getData()), is(2));
    }

    @Test
    void receivePlaysWellWithSeek() throws Exception {
        produceZeroToNineWithoutSchema();
        MessageId thirdMessageId = getThirdMessage().getMessageId();

        Mono<Message<byte[]>> fifthMessage = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(randomSubscriptionName())
                .forOne(consumer -> consumer.receive()
                        .then(consumer.seek(thirdMessageId))
                        .then(consumer.receive())
                        .then(consumer.receive())
                );

        StepVerifier.create(fifthMessage)
                .assertNext(message -> assertThat(intFromBytes(message.getData()), is(4)))
                .verifyComplete();
    }

    @Test
    void unacknowledgedMessageIsDeliveredAgain() throws Exception {
        produceZeroToNineWithoutSchema();
        receiveOneMessageWithoutAck(subscriptionName);

        Message<byte[]> message = receiveOneMessageWithoutAck(subscriptionName);

        assertThat(converter.intFromBytes(message.getData()), is(0));
    }

    @NotNull
    private Message<byte[]> receiveOneMessageWithoutAck(String subscriptionName) {
        Message<byte[]> message = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subscriptionName)
                .forOne(ReactiveConsumer::receive)
                .block();
        assertThat(message, notNullValue());
        return message;
    }

    @Test
    void acknowledgeMessageOnConsumerWorks() throws Exception {
        produceZeroToNineWithoutSchema();

        reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subscriptionName)
                .forOne(consumer -> consumer.receive().flatMap(consumer::acknowledge))
                .block();

        Message<byte[]> message = receiveOneMessageWithoutAck(subscriptionName);
        assertThat(converter.intFromBytes(message.getData()), is(1));
    }

    @Test
    void acknowledgeMessageIdOnConsumerWorks() throws Exception {
        produceZeroToNineWithoutSchema();

        reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subscriptionName)
                .forOne(consumer -> consumer.receive()
                        .flatMap(message -> consumer.acknowledge(message.getMessageId()))
                )
                .block();

        Message<byte[]> message = receiveOneMessageWithoutAck(subscriptionName);
        assertThat(converter.intFromBytes(message.getData()), is(1));
    }

    @Test
    @Disabled("Enable when the reason of hanging up is revealed")
    void acknowledgeMessagesOnConsumerWorks() throws Exception {
        produceZeroToNineWithoutSchema();

        reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subscriptionName)
                .forOne(consumer -> consumer.batchReceive().flatMap(consumer::acknowledge))
                .block();

        Message<byte[]> message = receiveOneMessageWithoutAck(subscriptionName);
        assertThat(converter.intFromBytes(message.getData()), is(1));
    }

    @Test
    void acknowledgeMessageIdListOnConsumerWorks() throws Exception {
        produceZeroToNineWithoutSchema();

        reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subscriptionName)
                .forOne(consumer -> consumer.receive()
                        .flatMap(message -> consumer.acknowledge(singletonList(message.getMessageId())))
                )
                .block();

        Message<byte[]> message = receiveOneMessageWithoutAck(subscriptionName);
        assertThat(converter.intFromBytes(message.getData()), is(1));
    }

    @Test
    void acknowledgeCumulativeMessageOnConsumerWorks() throws Exception {
        produceZeroToNineWithoutSchema();

        reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subscriptionName)
                .forOne(consumer -> consumer.receive().flatMap(consumer::acknowledgeCumulative))
                .block();

        Message<byte[]> message = receiveOneMessageWithoutAck(subscriptionName);
        assertThat(converter.intFromBytes(message.getData()), is(1));
    }

    @Test
    void acknowledgeCumulativeMessageIdOnConsumerWorks() throws Exception {
        produceZeroToNineWithoutSchema();

        reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subscriptionName)
                .forOne(consumer -> consumer.receive()
                        .flatMap(message -> consumer.acknowledgeCumulative(message.getMessageId()))
                )
                .block();

        Message<byte[]> message = receiveOneMessageWithoutAck(subscriptionName);
        assertThat(converter.intFromBytes(message.getData()), is(1));
    }

    @Test
    void getLastMessageIdWorks() throws Exception {
        List<MessageId> producedMessageIds = produceZeroToNineWithoutSchema(false);
        MessageId lastMessageId = producedMessageIds.get(producedMessageIds.size() - 1);

        MessageId lastMessageIdFromCall = reactiveClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subscriptionName)
                .forOne(ReactiveConsumer::getLastMessageId)
                .block();

        assertThat(lastMessageIdFromCall, comparesEqualTo(lastMessageId));
    }
}
