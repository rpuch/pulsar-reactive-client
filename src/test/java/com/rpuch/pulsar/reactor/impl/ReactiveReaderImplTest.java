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
package com.rpuch.pulsar.reactor.impl;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveReaderImplTest {
    @InjectMocks
    private ReactiveReaderImpl<String> reactiveReader;

    @Mock
    private Reader<String> coreReader;

    @Test
    void getTopicConsultsWithCoreReader() {
        when(coreReader.getTopic()).thenReturn("a");

        assertThat(reactiveReader.getTopic(), is("a"));
    }

    @Test
    void messagesReceivesMessagesFromRepetitiveReadNextAsync() {
        when(coreReader.readNextAsync()).then(NextMessageAnswer.produce("a", "b"));

        reactiveReader.messages()
                .as(StepVerifier::create)
                .assertNext(message -> assertThat(message.getValue(), is("a")))
                .assertNext(message -> assertThat(message.getValue(), is("b")))
                .expectTimeout(Duration.ofMillis(100))
                .verify();
    }

    @Test
    void messagesConvertsFailureToError() {
        Exception exception = new Exception("Oops");
        when(coreReader.readNextAsync()).then(NextMessageAnswer.failWith(exception));

        reactiveReader.messages()
                .as(StepVerifier::create)
                .expectErrorSatisfies(ex -> assertThat(ex, sameInstance(exception)))
                .verify();
    }

    @Test
    void onlyCallsReadNextAsyncWhenDemandIsRequestedThroughFluxReturnedByMessages() {
        when(coreReader.readNextAsync()).then(NextMessageAnswer.produce("a", "b"));

        Flux<Message<String>> messages = reactiveReader.messages();
        requestExactlyOneMessage(messages);

        verify(coreReader, times(1)).readNextAsync();
    }

    private void requestExactlyOneMessage(Flux<Message<String>> messages) {
        messages.limitRequest(1).blockFirst();
    }

    @Test
    void readNextReceivesMessageFromReadNextAsync() {
        when(coreReader.readNextAsync()).then(NextMessageAnswer.produce("a", "b"));

        reactiveReader.readNext()
                .as(StepVerifier::create)
                .assertNext(message -> assertThat(message.getValue(), is("a")))
                .verifyComplete();
    }

    @Test
    void readNextConvertsFailureToError() {
        Exception exception = new Exception("Oops");
        when(coreReader.readNextAsync()).then(NextMessageAnswer.failWith(exception));

        reactiveReader.readNext()
                .as(StepVerifier::create)
                .expectErrorSatisfies(ex -> assertThat(ex, sameInstance(exception)))
                .verify();
    }

    @Test
    void hasReachedEndOfTopicConsultsWithCoreReader() {
        when(coreReader.hasReachedEndOfTopic()).thenReturn(true);

        assertThat(reactiveReader.hasReachedEndOfTopic(), is(true));
    }

    @Test
    void hasMessageAvailableTakesDataFromReaderHasMessageAvailableAsync() {
        when(coreReader.hasMessageAvailableAsync()).thenReturn(completedFuture(true));

        reactiveReader.hasMessageAvailable()
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void isConnectedConsultsWithCoreReader() {
        when(coreReader.isConnected()).thenReturn(true);

        assertThat(reactiveReader.isConnected(), is(true));
    }

    @Test
    void seekByMessageIdCallsCorrespondingAsyncMethodOnCoreReader() {
        MessageId messageId = mock(MessageId.class);
        when(coreReader.seekAsync(messageId)).thenReturn(completedFuture(null));

        reactiveReader.seek(messageId).block();

        verify(coreReader).seekAsync(messageId);
    }

    @Test
    void seekByTimestampCallsCorrespondingAsyncMethodOnCoreReader() {
        when(coreReader.seekAsync(123)).thenReturn(completedFuture(null));

        reactiveReader.seek(123).block();

        verify(coreReader).seekAsync(123);
    }
}