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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import static com.rpuch.pulsar.reactor.utils.Futures.failedFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveProducerImplTest {
    @InjectMocks
    private ReactiveProducerImpl<String> reactiveProducer;

    @Mock
    private Producer<String> coreProducer;

    @Mock
    private TypedMessageBuilder<String> messageBuilder;

    @Mock
    private MessageId messageId;

    @Test
    void getTopicConsultsCoreProducer() {
        when(coreProducer.getTopic()).thenReturn("a");

        assertThat(reactiveProducer.getTopic(), is("a"));
    }

    @Test
    void getProducerNameConsultsCoreProducer() {
        when(coreProducer.getProducerName()).thenReturn("a");

        assertThat(reactiveProducer.getProducerName(), is("a"));
    }

    @Test
    void sendUsesSendAsync() {
        when(coreProducer.sendAsync("payload")).thenReturn(completedFuture(messageId));

        reactiveProducer.send("payload")
                .as(StepVerifier::create)
                .expectNext(messageId)
                .verifyComplete();
    }

    @Test
    void sendRelaysErrorFromFutureFailure() {
        RuntimeException exception = new RuntimeException("Oops");
        when(coreProducer.sendAsync("payload")).thenReturn(failedFuture(exception));

        reactiveProducer.send("payload")
                .as(StepVerifier::create)
                .expectErrorSatisfies(ex -> assertThat(ex, sameInstance(exception)))
                .verify();
    }

    @Test
    void flushUsesSendAsync() {
        when(coreProducer.flushAsync()).thenReturn(completedFuture(null));

        reactiveProducer.flush()
                .as(StepVerifier::create)
                .verifyComplete();

        verify(coreProducer).flushAsync();
    }

    @Test
    void newMessageReturnsABuilderCooperatingWithCoreProducer() {
        when(coreProducer.newMessage()).thenReturn(messageBuilder);

        assertThat(reactiveProducer.newMessage(), notNullValue());

        verify(coreProducer).newMessage();
    }

    @Test
    void newMessageWithSchemaReturnsABuilderCooperatingWithCoreProducer() {
        when(coreProducer.newMessage(any(StringSchema.class))).thenReturn(messageBuilder);

        assertThat(reactiveProducer.newMessage(Schema.STRING), notNullValue());
    }

    @Test
    void getLastSequenceIdConsultsCoreProducer() {
        when(coreProducer.getLastSequenceId()).thenReturn(123L);

        assertThat(reactiveProducer.getLastSequenceId(), is(123L));
    }

    @Test
    void getStatsConsultsCoreProducer() {
        ProducerStats stats = mock(ProducerStats.class);
        when(coreProducer.getStats()).thenReturn(stats);

        assertThat(reactiveProducer.getStats(), sameInstance(stats));
    }

    @Test
    void isConnectedConsultsCoreProducer() {
        when(coreProducer.isConnected()).thenReturn(true);

        assertThat(reactiveProducer.isConnected(), is(true));
    }

    @Test
    void getLastDisconnectedTimestampConsultsCoreProducer() {
        when(coreProducer.getLastDisconnectedTimestamp()).thenReturn(123L);

        assertThat(reactiveProducer.getLastDisconnectedTimestamp(), is(123L));
    }
}