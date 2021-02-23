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
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.util.concurrent.TimeUnit;

import static com.rpuch.pulsar.reactor.utils.Futures.failedFuture;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveTypedMessageBuilderImplTest {
    @InjectMocks
    private ReactiveTypedMessageBuilderImpl<String> reactiveBuilder;

    @Mock
    private TypedMessageBuilder<String> coreBuilder;

    @Mock
    private MessageId messageId;

    @Test
    void sendUsesSendAsync() {
        when(coreBuilder.sendAsync()).thenReturn(completedFuture(messageId));

        reactiveBuilder.send()
                .as(StepVerifier::create)
                .expectNext(messageId)
                .verifyComplete();
    }

    @Test
    void sendRelaysErrorFromFutureFailure() {
        RuntimeException exception = new RuntimeException("Oops");
        when(coreBuilder.sendAsync()).thenReturn(failedFuture(exception));

        reactiveBuilder.send()
                .as(StepVerifier::create)
                .expectErrorSatisfies(ex -> assertThat(ex, sameInstance(exception)))
                .verify();
    }

    @Test
    void setsKeyOnCoreBuilder() {
        reactiveBuilder.key("key");

        verify(coreBuilder).key("key");
    }

    @Test
    void keyReturnsSameBuilder() {
        assertThat(reactiveBuilder.key("key"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsKeyBytesOnCoreBuilder() {
        byte[] keyBytes = new byte[0];
        reactiveBuilder.keyBytes(keyBytes);

        verify(coreBuilder).keyBytes(keyBytes);
    }

    @Test
    void keyBytesReturnsSameBuilder() {
        assertThat(reactiveBuilder.keyBytes(new byte[0]), sameInstance(reactiveBuilder));
    }

    @Test
    void setsOrderingKeyOnCoreBuilder() {
        byte[] keyBytes = new byte[0];
        reactiveBuilder.orderingKey(keyBytes);

        verify(coreBuilder).orderingKey(keyBytes);
    }

    @Test
    void orderingKeyReturnsSameBuilder() {
        assertThat(reactiveBuilder.orderingKey(new byte[0]), sameInstance(reactiveBuilder));
    }

    @Test
    void setsValueOnCoreBuilder() {
        reactiveBuilder.value("v");

        verify(coreBuilder).value("v");
    }

    @Test
    void valueReturnsSameBuilder() {
        assertThat(reactiveBuilder.value("v"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsPropertyOnCoreBuilder() {
        reactiveBuilder.property("key", "value");

        verify(coreBuilder).property("key", "value");
    }

    @Test
    void propertyReturnsSameBuilder() {
        assertThat(reactiveBuilder.property("key", "value"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsPropertiesOnCoreBuilder() {
        reactiveBuilder.properties(emptyMap());

        verify(coreBuilder).properties(emptyMap());
    }

    @Test
    void propertiesReturnsSameBuilder() {
        assertThat(reactiveBuilder.properties(emptyMap()), sameInstance(reactiveBuilder));
    }

    @Test
    void setsEventTimeOnCoreBuilder() {
        reactiveBuilder.eventTime(123);

        verify(coreBuilder).eventTime(123);
    }

    @Test
    void eventTimeReturnsSameBuilder() {
        assertThat(reactiveBuilder.eventTime(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsSequenceIdOnCoreBuilder() {
        reactiveBuilder.sequenceId(123);

        verify(coreBuilder).sequenceId(123);
    }

    @Test
    void sequenceIdReturnsSameBuilder() {
        assertThat(reactiveBuilder.sequenceId(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsReplicationClustersOnCoreBuilder() {
        reactiveBuilder.replicationClusters(singletonList("a"));

        verify(coreBuilder).replicationClusters(singletonList("a"));
    }

    @Test
    void replicationClustersReturnsSameBuilder() {
        assertThat(reactiveBuilder.replicationClusters(singletonList("a")), sameInstance(reactiveBuilder));
    }

    @Test
    void setsDisableReplicationOnCoreBuilder() {
        reactiveBuilder.disableReplication();

        verify(coreBuilder).disableReplication();
    }

    @Test
    void disableReplicationReturnsSameBuilder() {
        assertThat(reactiveBuilder.disableReplication(), sameInstance(reactiveBuilder));
    }

    @Test
    void setsDeliverAtOnCoreBuilder() {
        reactiveBuilder.deliverAt(123);

        verify(coreBuilder).deliverAt(123);
    }

    @Test
    void deliverAtReturnsSameBuilder() {
        assertThat(reactiveBuilder.deliverAt(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsDeliverAfterOnCoreBuilder() {
        reactiveBuilder.deliverAfter(123, TimeUnit.SECONDS);

        verify(coreBuilder).deliverAfter(123, TimeUnit.SECONDS);
    }

    @Test
    void deliverAfterReturnsSameBuilder() {
        assertThat(reactiveBuilder.deliverAfter(123, TimeUnit.SECONDS), sameInstance(reactiveBuilder));
    }

    @Test
    void invokesLoadConfOnCoreBuilder() {
        reactiveBuilder.loadConf(singletonMap("k", "v"));

        verify(coreBuilder).loadConf(singletonMap("k", "v"));
    }

    @Test
    void loadConfReturnsSameBuilder() {
        assertThat(reactiveBuilder.loadConf(singletonMap("k", "v")), sameInstance(reactiveBuilder));
    }
}