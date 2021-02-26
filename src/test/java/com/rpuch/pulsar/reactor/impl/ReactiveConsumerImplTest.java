package com.rpuch.pulsar.reactor.impl;

import com.rpuch.pulsar.reactor.utils.Futures;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveConsumerImplTest {
    @InjectMocks
    private ReactiveConsumerImpl<String> reactiveConsumer;
    
    @Mock
    private Consumer<String> coreConsumer;

    @Mock
    private Message<String> message;
    @Mock
    private Messages<String> messages;
    @Mock
    private MessageId messageId;
    
    @Test
    void getTopicConsultsWithCoreConsumer() {
        when(coreConsumer.getTopic()).thenReturn("a");

        assertThat(reactiveConsumer.getTopic(), is("a"));
    }
    
    @Test
    void getSubscriptionConsultsWithCoreConsumer() {
        when(coreConsumer.getSubscription()).thenReturn("a");

        assertThat(reactiveConsumer.getSubscription(), is("a"));
    }

    @Test
    void unsubscribeCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.unsubscribeAsync()).thenReturn(completedFuture(null));

        reactiveConsumer.unsubscribe().block();

        verify(coreConsumer).unsubscribeAsync();
    }
    
    @Test
    void receiveCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.receiveAsync()).thenReturn(completedFuture(message));

        reactiveConsumer.receive()
                .as(StepVerifier::create)
                .expectNext(message)
                .verifyComplete();
    }

    @Test
    void receiveConvertsFutureFailureToError() {
        RuntimeException exception = new RuntimeException("Oops");
        when(coreConsumer.receiveAsync()).thenReturn(Futures.failedFuture(exception));

        reactiveConsumer.receive()
                .as(StepVerifier::create)
                .expectErrorSatisfies(ex -> assertThat(ex, sameInstance(exception)))
                .verify();
    }

    @Test
    void batchReceiveCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.batchReceiveAsync()).thenReturn(completedFuture(messages));

        reactiveConsumer.batchReceive()
                .as(StepVerifier::create)
                .expectNext(messages)
                .verifyComplete();
    }

    @Test
    void batchReceiveConvertsFutureFailureToError() {
        RuntimeException exception = new RuntimeException("Oops");
        when(coreConsumer.batchReceiveAsync()).thenReturn(Futures.failedFuture(exception));

        reactiveConsumer.batchReceive()
                .as(StepVerifier::create)
                .expectErrorSatisfies(ex -> assertThat(ex, sameInstance(exception)))
                .verify();
    }

    @Test
    void acknowledgeMessageCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.acknowledgeAsync(message)).thenReturn(completedFuture(null));

        reactiveConsumer.acknowledge(message).block();

        verify(coreConsumer).acknowledgeAsync(message);
    }

    @Test
    void acknowledgeMessageIdCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.acknowledgeAsync(messageId)).thenReturn(completedFuture(null));

        reactiveConsumer.acknowledge(messageId).block();

        verify(coreConsumer).acknowledgeAsync(messageId);
    }

    @Test
    void acknowledgeMessagesCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.acknowledgeAsync(messages)).thenReturn(completedFuture(null));

        reactiveConsumer.acknowledge(messages).block();

        verify(coreConsumer).acknowledgeAsync(messages);
    }

    @Test
    void acknowledgeMessageIdListCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.acknowledgeAsync(singletonList(messageId))).thenReturn(completedFuture(null));

        reactiveConsumer.acknowledge(singletonList(messageId)).block();

        verify(coreConsumer).acknowledgeAsync(singletonList(messageId));
    }

    @Test
    void negativeAcknowledgeMessageIsRelayedToCoreConsumer() {
        reactiveConsumer.negativeAcknowledge(message);

        verify(coreConsumer).negativeAcknowledge(message);
    }

    @Test
    void negativeAcknowledgeMessageIdIsRelayedToCoreConsumer() {
        reactiveConsumer.negativeAcknowledge(messageId);

        verify(coreConsumer).negativeAcknowledge(messageId);
    }

    @Test
    void negativeAcknowledgeMessagesIsRelayedToCoreConsumer() {
        reactiveConsumer.negativeAcknowledge(messages);

        verify(coreConsumer).negativeAcknowledge(messages);
    }

    @Test
    void reconsumeLaterMessageCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.reconsumeLaterAsync(message, 1, TimeUnit.SECONDS))
                .thenReturn(completedFuture(null));

        reactiveConsumer.reconsumeLater(message, 1, TimeUnit.SECONDS).block();

        verify(coreConsumer).reconsumeLaterAsync(message, 1, TimeUnit.SECONDS);
    }

    @Test
    void reconsumeLaterMessagesCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.reconsumeLaterAsync(messages, 1, TimeUnit.SECONDS))
                .thenReturn(completedFuture(null));

        reactiveConsumer.reconsumeLater(messages, 1, TimeUnit.SECONDS).block();

        verify(coreConsumer).reconsumeLaterAsync(messages, 1, TimeUnit.SECONDS);
    }

    @Test
    void acknowledgeCumulativeMessageCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.acknowledgeCumulativeAsync(message))
                .thenReturn(completedFuture(null));

        reactiveConsumer.acknowledgeCumulative(message).block();

        verify(coreConsumer).acknowledgeCumulativeAsync(message);
    }

    @Test
    void acknowledgeCumulativeMessageIdCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.acknowledgeCumulativeAsync(messageId))
                .thenReturn(completedFuture(null));

        reactiveConsumer.acknowledgeCumulative(messageId).block();

        verify(coreConsumer).acknowledgeCumulativeAsync(messageId);
    }

    @Test
    void reconsumeLaterCumulativeCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.reconsumeLaterCumulativeAsync(message, 1, TimeUnit.SECONDS))
                .thenReturn(completedFuture(null));

        reactiveConsumer.reconsumeLaterCumulative(message, 1, TimeUnit.SECONDS).block();

        verify(coreConsumer).reconsumeLaterCumulativeAsync(message, 1, TimeUnit.SECONDS);
    }

    @Test
    void getStatsConsultsWithCoreConsumer() {
        ConsumerStats stats = mock(ConsumerStats.class);
        when(coreConsumer.getStats()).thenReturn(stats);

        assertThat(reactiveConsumer.getStats(), sameInstance(stats));
    }

    @Test
    void hasReachedEndOfTopicConsultsWithCoreConsumer() {
        when(coreConsumer.hasReachedEndOfTopic()).thenReturn(true);

        assertThat(reactiveConsumer.hasReachedEndOfTopic(), is(true));
    }

    @Test
    void redeliverUnacknowledgedMessagesCallsSameMethodOnCoreConsumer() {
        reactiveConsumer.redeliverUnacknowledgedMessages();

        verify(coreConsumer).redeliverUnacknowledgedMessages();
    }

    @Test
    void getLastMessageIdReturnsResultFromGetLastMessageIdAsync() {
        when(coreConsumer.getLastMessageIdAsync()).thenReturn(completedFuture(messageId));

        reactiveConsumer.getLastMessageId()
                .as(StepVerifier::create)
                .expectNext(messageId)
                .verifyComplete();
    }

    @Test
    void seekByMessageIdCallsCorrespondingAsyncMethodOnCoreConsumer() {
        MessageId messageId = mock(MessageId.class);
        when(coreConsumer.seekAsync(messageId)).thenReturn(completedFuture(null));

        reactiveConsumer.seek(messageId).block();

        verify(coreConsumer).seekAsync(messageId);
    }

    @Test
    void seekByTimestampCallsCorrespondingAsyncMethodOnCoreConsumer() {
        when(coreConsumer.seekAsync(123)).thenReturn(completedFuture(null));

        reactiveConsumer.seek(123).block();

        verify(coreConsumer).seekAsync(123);
    }

    @Test
    void isConnectedConsultsWithCoreConsumer() {
        when(coreConsumer.isConnected()).thenReturn(true);

        assertThat(reactiveConsumer.isConnected(), is(true));
    }

    @Test
    void getConsumerNameConsultsWithCoreConsumer() {
        when(coreConsumer.getConsumerName()).thenReturn("a");

        assertThat(reactiveConsumer.getConsumerName(), is("a"));
    }

    @Test
    void pausePausesCoreConsumer() {
        reactiveConsumer.pause();

        verify(coreConsumer).pause();
    }

    @Test
    void resumeResumesCoreConsumer() {
        reactiveConsumer.resume();

        verify(coreConsumer).resume();
    }

    @Test
    void getLastDisconnectedTimestampConsultsWithCoreConsumer() {
        when(coreConsumer.getLastDisconnectedTimestamp()).thenReturn(123L);

        assertThat(reactiveConsumer.getLastDisconnectedTimestamp(), is(123L));
    }
}