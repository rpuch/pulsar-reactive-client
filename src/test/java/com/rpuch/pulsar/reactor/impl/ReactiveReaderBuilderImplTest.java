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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.rpuch.pulsar.reactor.impl.NextMessageAnswer.failWith;
import static com.rpuch.pulsar.reactor.impl.NextMessageAnswer.produce;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.pulsar.client.api.ConsumerCryptoFailureAction.FAIL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveReaderBuilderImplTest {
    @InjectMocks
    private ReactiveReaderBuilderImpl<String> reactiveBuilder;

    @Mock
    private ReaderBuilder<String> coreBuilder;

    @Mock
    private Reader<String> coreReader;
    
    @Test
    void messagesReadsFromCoreReaderUsingReadNextAsync() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.readNextAsync()).then(produce("a", "b"));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        List<String> result = reactiveBuilder.messages()
                .map(Message::getValue)
                .take(2)
                .toStream().collect(toList());

        assertThat(result, equalTo(Arrays.asList("a", "b")));
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByMessagesCompletesNormally() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.readNextAsync()).then(produce("a", "b"));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.messages().take(2).blockLast();

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByMessagesCompletesWithError() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.readNextAsync()).then(failWith(new RuntimeException("Oops")));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class, () -> reactiveBuilder.messages().blockLast());

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByMessagesIsCancelled() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.readNextAsync()).then(produce("a", "b"));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = reactiveBuilder.messages().concatMap(x -> Mono.never()).subscribe();
        disposable.dispose();

        verify(coreReader).closeAsync();
    }
    
    @Test
    void forOneReturnsResultOfInternalTransformation() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forOne(reader -> Mono.just("a"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneCompletesNormally() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forOne(reader -> Mono.just("a")).block();

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneCompletesWithError() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class,
                () -> reactiveBuilder.forOne(reader -> Mono.error(new RuntimeException("Oops"))).block());

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneIsCancelled() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = reactiveBuilder.forOne(reader -> Mono.just("a")).subscribe();
        disposable.dispose();

        verify(coreReader).closeAsync();
    }

    @Test
    void forManyReturnsResultOfInternalTransformation() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forMany(reader -> Flux.just("a"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyCompletesNormally() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forMany(reader -> Flux.just("a")).blockLast();

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyCompletesWithError() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class,
                () -> reactiveBuilder.forMany(reader -> Flux.error(new RuntimeException("Oops"))).blockLast());

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyIsCancelled() {
        when(coreBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = reactiveBuilder.forMany(reader -> Flux.just("a")).subscribe();
        disposable.dispose();

        verify(coreReader).closeAsync();
    }
    
    @Test
    void invokesLoadConfOnCoreBuilder() {
        reactiveBuilder.loadConf(emptyMap());

        verify(coreBuilder).loadConf(emptyMap());
    }

    @Test
    void loadConfReturnsSameBuilder() {
        assertThat(reactiveBuilder.loadConf(singletonMap("k", "v")), sameInstance(reactiveBuilder));
    }

    @Test
    void setsTopicOnCoreBuilder() {
        reactiveBuilder.topic("a");

        verify(coreBuilder).topic("a");
    }

    @Test
    void topicReturnsSameBuilder() {
        assertThat(reactiveBuilder.topic("test"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsStartMessageIdOnCoreBuilder() {
        reactiveBuilder.startMessageId(MessageId.earliest);

        verify(coreBuilder).startMessageId(MessageId.earliest);
    }

    @Test
    void startMessageIdReturnsSameBuilder() {
        assertThat(reactiveBuilder.startMessageId(MessageId.earliest), sameInstance(reactiveBuilder));
    }

    @Test
    void setsStartMessageFromRollbackDurationOnCoreBuilder() {
        reactiveBuilder.startMessageFromRollbackDuration(777, TimeUnit.MILLISECONDS);

        verify(coreBuilder).startMessageFromRollbackDuration(777, TimeUnit.MILLISECONDS);
    }

    @Test
    void startMessageFromRollbackDurationReturnsSameBuilder() {
        assertThat(reactiveBuilder.startMessageFromRollbackDuration(123, TimeUnit.SECONDS),
                sameInstance(reactiveBuilder));
    }

    @Test
    void setsStartMessageIdInclusiveOnCoreBuilder() {
        reactiveBuilder.startMessageIdInclusive();

        verify(coreBuilder).startMessageIdInclusive();
    }

    @Test
    void startMessageIdInclusiveReturnsSameBuilder() {
        assertThat(reactiveBuilder.startMessageIdInclusive(), sameInstance(reactiveBuilder));
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
    void setsCryptoFailureActionOnCoreBuilder() {
        reactiveBuilder.cryptoFailureAction(FAIL);

        verify(coreBuilder).cryptoFailureAction(FAIL);
    }

    @Test
    void cryptoFailureActionReturnsSameBuilder() {
        assertThat(reactiveBuilder.cryptoFailureAction(FAIL), sameInstance(reactiveBuilder));
    }

    @Test
    void setsReceiverQueueSizeOnCoreBuilder() {
        reactiveBuilder.receiverQueueSize(1);

        verify(coreBuilder).receiverQueueSize(1);
    }

    @Test
    void receiverQueueSizeReturnsSameBuilder() {
        assertThat(reactiveBuilder.receiverQueueSize(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsReaderNameOnCoreBuilder() {
        reactiveBuilder.readerName("name");

        verify(coreBuilder).readerName("name");
    }

    @Test
    void readerNameReturnsSameBuilder() {
        assertThat(reactiveBuilder.readerName("test"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsSubscriptionRolePrefixOnCoreBuilder() {
        reactiveBuilder.subscriptionRolePrefix("prefix");

        verify(coreBuilder).subscriptionRolePrefix("prefix");
    }

    @Test
    void subscriptionRolePrefixReturnsSameBuilder() {
        assertThat(reactiveBuilder.subscriptionRolePrefix("test"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsReadCompactedOnCoreBuilder() {
        reactiveBuilder.readCompacted(true);

        verify(coreBuilder).readCompacted(true);
    }

    @Test
    void readCompactedReturnsSameBuilder() {
        assertThat(reactiveBuilder.readCompacted(true), sameInstance(reactiveBuilder));
    }

    @Test
    void setsKeyHashRangeOnCoreBuilder() {
        Range range = Range.of(0, 1);
        
        reactiveBuilder.keyHashRange(range);

        verify(coreBuilder).keyHashRange(range);
    }

    @Test
    void keyHashRangeReturnsSameBuilder() {
        assertThat(reactiveBuilder.keyHashRange(Range.of(0, 1)), sameInstance(reactiveBuilder));
    }
}