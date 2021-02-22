package com.rpuch.pulsar.reactive.impl;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.rpuch.pulsar.reactive.impl.NextMessageAnswer.failWith;
import static com.rpuch.pulsar.reactive.impl.NextMessageAnswer.produce;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.pulsar.client.api.ConsumerCryptoFailureAction.FAIL;
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

        Assertions.assertThrows(RuntimeException.class, () -> readerBuilder.receive().blockLast());

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
}