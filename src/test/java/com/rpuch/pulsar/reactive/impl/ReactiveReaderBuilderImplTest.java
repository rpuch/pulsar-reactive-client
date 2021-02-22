package com.rpuch.pulsar.reactive.impl;

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

import static com.rpuch.pulsar.reactive.impl.NextMessageAnswer.failWith;
import static com.rpuch.pulsar.reactive.impl.NextMessageAnswer.produce;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.pulsar.client.api.ConsumerCryptoFailureAction.FAIL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
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
    private ReactiveReaderBuilderImpl<String> readerBuilder;

    @Mock
    private ReaderBuilder<String> coreReaderBuilder;

    @Mock
    private Reader<String> coreReader;
    
    @Test
    void receiveReadsFromCoreReaderUsingReadNextAsync() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.readNextAsync()).then(produce("a", "b"));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        List<String> result = readerBuilder.receive()
                .map(Message::getValue)
                .take(2)
                .toStream().collect(toList());

        assertThat(result, equalTo(Arrays.asList("a", "b")));
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByReceiveCompletesNormally() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.readNextAsync()).then(produce("a", "b"));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        readerBuilder.receive().take(2).blockLast();

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByReceiveCompletesWithError() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.readNextAsync()).then(failWith(new RuntimeException("Oops")));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class, () -> readerBuilder.receive().blockLast());

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByReceiveIsCancelled() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.readNextAsync()).then(produce("a", "b"));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = readerBuilder.receive().concatMap(x -> Mono.never()).subscribe();
        disposable.dispose();

        verify(coreReader).closeAsync();
    }
    
    @Test
    void forOneReturnsResullOfInternalTransformation() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        readerBuilder.forOne(reader -> Mono.just("a"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneCompletesNormally() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        readerBuilder.forOne(reader -> Mono.just("a")).block();

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneCompletesWithError() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class,
                () -> readerBuilder.forOne(reader -> Mono.error(new RuntimeException("Oops"))).block());

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToMonoReturnedByForOneIsCancelled() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = readerBuilder.forOne(reader -> Mono.just("a")).subscribe();
        disposable.dispose();

        verify(coreReader).closeAsync();
    }

    @Test
    void forManyReturnsResullOfInternalTransformation() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        readerBuilder.forMany(reader -> Flux.just("a"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyCompletesNormally() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        readerBuilder.forMany(reader -> Flux.just("a")).blockLast();

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyCompletesWithError() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class,
                () -> readerBuilder.forMany(reader -> Flux.error(new RuntimeException("Oops"))).blockLast());

        verify(coreReader).closeAsync();
    }

    @Test
    void closesCoreReaderAfterASubscriptionToFluxReturnedByForManyIsCancelled() {
        when(coreReaderBuilder.createAsync()).thenReturn(completedFuture(coreReader));
        when(coreReader.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = readerBuilder.forMany(reader -> Flux.just("a")).subscribe();
        disposable.dispose();

        verify(coreReader).closeAsync();
    }
    
    @Test
    void invokesLoadConfOnCoreReader() {
        readerBuilder.loadConf(emptyMap());

        verify(coreReaderBuilder).loadConf(emptyMap());
    }

    @Test
    void setsTopicOnCoreReader() {
        readerBuilder.topic("a");

        verify(coreReaderBuilder).topic("a");
    }

    @Test
    void setsStartMessageIdOnCoreReader() {
        readerBuilder.startMessageId(MessageId.earliest);

        verify(coreReaderBuilder).startMessageId(MessageId.earliest);
    }

    @Test
    void setsstartMessageFromRollbackDurationOnCoreReader() {
        readerBuilder.startMessageFromRollbackDuration(777, TimeUnit.MILLISECONDS);

        verify(coreReaderBuilder).startMessageFromRollbackDuration(777, TimeUnit.MILLISECONDS);
    }

    @Test
    void setsStartMessageIdInclusiveOnCoreReader() {
        readerBuilder.startMessageIdInclusive();

        verify(coreReaderBuilder).startMessageIdInclusive();
    }

    @Test
    void setsCryptoKeyReaderOnCoreReader() {
        CryptoKeyReader cryptoKeyReader = mock(CryptoKeyReader.class);

        readerBuilder.cryptoKeyReader(cryptoKeyReader);

        verify(coreReaderBuilder).cryptoKeyReader(cryptoKeyReader);
    }

    @Test
    void setsCryptoFailureActionOnCoreReader() {
        readerBuilder.cryptoFailureAction(FAIL);

        verify(coreReaderBuilder).cryptoFailureAction(FAIL);
    }

    @Test
    void setsReceiverQueueSizeOnCoreReader() {
        readerBuilder.receiverQueueSize(1);

        verify(coreReaderBuilder).receiverQueueSize(1);
    }

    @Test
    void setsReaderNameOnCoreReader() {
        readerBuilder.readerName("name");

        verify(coreReaderBuilder).readerName("name");
    }

    @Test
    void setsSubscriptionRolePrefixOnCoreReader() {
        readerBuilder.subscriptionRolePrefix("prefix");

        verify(coreReaderBuilder).subscriptionRolePrefix("prefix");
    }

    @Test
    void setsReadCompactedOnCoreReader() {
        readerBuilder.readCompacted(true);

        verify(coreReaderBuilder).readCompacted(true);
    }

    @Test
    void setsKeyHashRangeOnCoreReader() {
        Range range = Range.of(0, 1);
        
        readerBuilder.keyHashRange(range);

        verify(coreReaderBuilder).keyHashRange(range);
    }
}