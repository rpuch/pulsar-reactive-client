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

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.pulsar.client.api.CompressionType.NONE;
import static org.apache.pulsar.client.api.ProducerCryptoFailureAction.FAIL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveProducerBuilderImplTest {
    @InjectMocks
    private ReactiveProducerBuilderImpl<String> reactiveBuilder;

    @Mock
    private ProducerBuilder<String> coreBuilder;

    @Mock
    private Producer<String> coreProducer;

    @Test
    void forOneReturnsResultOfInternalTransformation() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreProducer));
        when(coreProducer.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forOne(reader -> Mono.just("a"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneCompletesNormally() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreProducer));
        when(coreProducer.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forOne(reader -> Mono.just("a")).block();

        verify(coreProducer).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneCompletesWithError() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreProducer));
        when(coreProducer.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class,
                () -> reactiveBuilder.forOne(reader -> Mono.error(new RuntimeException("Oops"))).block());

        verify(coreProducer).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneIsCancelled() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreProducer));
        when(coreProducer.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = reactiveBuilder.forOne(reader -> Mono.just("a")).subscribe();
        disposable.dispose();

        verify(coreProducer).closeAsync();
    }

    @Test
    void forManyReturnsResultOfInternalTransformation() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreProducer));
        when(coreProducer.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forMany(reader -> Flux.just("a"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyCompletesNormally() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreProducer));
        when(coreProducer.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forMany(reader -> Flux.just("a")).blockLast();

        verify(coreProducer).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyCompletesWithError() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreProducer));
        when(coreProducer.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class,
                () -> reactiveBuilder.forMany(reader -> Flux.error(new RuntimeException("Oops"))).blockLast());

        verify(coreProducer).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyIsCancelled() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreProducer));
        when(coreProducer.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = reactiveBuilder.forMany(reader -> Flux.just("a")).subscribe();
        disposable.dispose();

        verify(coreProducer).closeAsync();
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
    
    @Test
    void setsTopicOnCoreReader() {
        reactiveBuilder.topic("a");

        verify(coreBuilder).topic("a");
    }

    @Test
    void topicReturnsSameBuilder() {
        assertThat(reactiveBuilder.topic("test"), sameInstance(reactiveBuilder));
    }
    
    @Test
    void setsProducerNameOnCoreBuilder() {
        reactiveBuilder.producerName("name");

        verify(coreBuilder).producerName("name");
    }

    @Test
    void producerNameReturnsSameBuilder() {
        assertThat(reactiveBuilder.producerName("test"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsSendTimeoutOnCoreBuilder() {
        reactiveBuilder.sendTimeout(1, TimeUnit.SECONDS);

        verify(coreBuilder).sendTimeout(1, TimeUnit.SECONDS);
    }

    @Test
    void sendTimeoutReturnsSameBuilder() {
        assertThat(reactiveBuilder.sendTimeout(1, TimeUnit.SECONDS), sameInstance(reactiveBuilder));
    }

    @Test
    void setsMaxPendingMessagesOnCoreBuilder() {
        reactiveBuilder.maxPendingMessages(123);

        verify(coreBuilder).maxPendingMessages(123);
    }

    @Test
    void maxPendingMessagesReturnsSameBuilder() {
        assertThat(reactiveBuilder.maxPendingMessages(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsMaxPendingMessagesAcrossPartitionsOnCoreBuilder() {
        reactiveBuilder.maxPendingMessagesAcrossPartitions(123);

        verify(coreBuilder).maxPendingMessagesAcrossPartitions(123);
    }

    @Test
    void maxPendingMessagesAcrossPartitionsReturnsSameBuilder() {
        assertThat(reactiveBuilder.maxPendingMessagesAcrossPartitions(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsMessageRoutingModeOnCoreBuilder() {
        reactiveBuilder.messageRoutingMode(MessageRoutingMode.RoundRobinPartition);

        verify(coreBuilder).messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
    }

    @Test
    void messageRoutingModeReturnsSameBuilder() {
        assertThat(reactiveBuilder.messageRoutingMode(MessageRoutingMode.RoundRobinPartition),
                sameInstance(reactiveBuilder));
    }

    @Test
    void setsHashingSchemeOnCoreBuilder() {
        reactiveBuilder.hashingScheme(HashingScheme.JavaStringHash);

        verify(coreBuilder).hashingScheme(HashingScheme.JavaStringHash);
    }

    @Test
    void hashingSchemeReturnsSameBuilder() {
        assertThat(reactiveBuilder.hashingScheme(HashingScheme.JavaStringHash), sameInstance(reactiveBuilder));
    }

    @Test
    void setsCompressionTypeOnCoreBuilder() {
        reactiveBuilder.compressionType(NONE);

        verify(coreBuilder).compressionType(NONE);
    }

    @Test
    void compressionTypeReturnsSameBuilder() {
        assertThat(reactiveBuilder.compressionType(NONE), sameInstance(reactiveBuilder));
    }

    @Test
    void setsMessgeRouterOnCoreBuilder() {
        MessageRouter router = mock(MessageRouter.class);

        reactiveBuilder.messageRouter(router);

        verify(coreBuilder).messageRouter(router);
    }

    @Test
    void messageRouterReturnsSameBuilder() {
        assertThat(reactiveBuilder.messageRouter(mock(MessageRouter.class)), sameInstance(reactiveBuilder));
    }

    @Test
    void setsCryptoKeyReaderOnCoreBuilder() {
        CryptoKeyReader cryptoKeyReader = mock(CryptoKeyReader.class);

        reactiveBuilder.cryptoKeyReader(cryptoKeyReader);

        verify(coreBuilder).cryptoKeyReader(cryptoKeyReader);
    }

    @Test
    void cryptoKeyReaderReturnsSameBuilder() {
        assertThat(reactiveBuilder.cryptoKeyReader(mock(CryptoKeyReader.class)), sameInstance(reactiveBuilder));
    }

    @Test
    void invokesAddEncryptionKeyOnCoreBuilder() {
        reactiveBuilder.addEncryptionKey("key");

        verify(coreBuilder).addEncryptionKey("key");
    }

    @Test
    void addEncryptionKeyReturnsSameBuilder() {
        assertThat(reactiveBuilder.addEncryptionKey("key"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsCryptoFailureActionOnCoreBuilder() {
        reactiveBuilder.cryptoFailureAction(FAIL);

        verify(coreBuilder).cryptoFailureAction(FAIL);
    }

    @Test
    void cryptoFailureActionReturnsSameBuilder() {
        assertThat(reactiveBuilder.cryptoFailureAction(FAIL), sameInstance(reactiveBuilder));
    }

    @Test
    void setsInitialSequenceIdOnCoreBuilder() {
        reactiveBuilder.initialSequenceId(123);

        verify(coreBuilder).initialSequenceId(123);
    }

    @Test
    void initialSequenceIdReturnsSameBuilder() {
        assertThat(reactiveBuilder.initialSequenceId(123), sameInstance(reactiveBuilder));
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
    void invokesInterceptOnCoreBuilder() {
        ProducerInterceptor interceptor = mock(ProducerInterceptor.class);

        reactiveBuilder.intercept(interceptor);

        verify(coreBuilder).intercept(interceptor);
    }

    @Test
    void interceptReturnsSameBuilder() {
        assertThat(reactiveBuilder.intercept(mock(ProducerInterceptor.class)), sameInstance(reactiveBuilder));
    }

    @Test
    void setsAutoUpdatePartitionsOnCoreBuilder() {
        reactiveBuilder.autoUpdatePartitions(true);

        verify(coreBuilder).autoUpdatePartitions(true);
    }

    @Test
    void autoUpdatePartitionsReturnsSameBuilder() {
        assertThat(reactiveBuilder.autoUpdatePartitions(true), sameInstance(reactiveBuilder));
    }

    @Test
    void setsAutoUpdatePartitionsIntervalOnCoreBuilder() {
        reactiveBuilder.autoUpdatePartitionsInterval(1, TimeUnit.SECONDS);

        verify(coreBuilder).autoUpdatePartitionsInterval(1, TimeUnit.SECONDS);
    }

    @Test
    void autoUpdatePartitionsIntervalReturnsSameBuilder() {
        assertThat(reactiveBuilder.autoUpdatePartitionsInterval(1, TimeUnit.SECONDS), sameInstance(reactiveBuilder));
    }

    @Test
    void setsEnableMultiSchemaOnCoreBuilder() {
        reactiveBuilder.enableMultiSchema(true);

        verify(coreBuilder).enableMultiSchema(true);
    }

    @Test
    void enableMultiSchemaReturnsSameBuilder() {
        assertThat(reactiveBuilder.enableMultiSchema(true), sameInstance(reactiveBuilder));
    }
}
