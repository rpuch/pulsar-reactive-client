package com.rpuch.pulsar.reactor.impl;

import com.rpuch.pulsar.reactor.api.ReactiveConsumerBuilder;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
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
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveConsumerBuilderImplTest {
    @InjectMocks
    private ReactiveConsumerBuilderImpl<String> reactiveBuilder;

    @Mock
    private ConsumerBuilder<String> coreBuilder;
    
    @Mock
    private Consumer<String> coreConsumer;

    @Test
    void forOneReturnsResultOfInternalTransformation() {
        when(coreBuilder.subscribeAsync()).thenReturn(completedFuture(coreConsumer));
        when(coreConsumer.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forOne(reader -> Mono.just("a"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();
    }

    @Test
    void closesCoreConsumerAfterASubscriptionToMonoReturnedByForOneCompletesNormally() {
        when(coreBuilder.subscribeAsync()).thenReturn(completedFuture(coreConsumer));
        when(coreConsumer.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forOne(reader -> Mono.just("a")).block();

        verify(coreConsumer).closeAsync();
    }

    @Test
    void closesCoreConsumerAfterASubscriptionToMonoReturnedByForOneCompletesWithError() {
        when(coreBuilder.subscribeAsync()).thenReturn(completedFuture(coreConsumer));
        when(coreConsumer.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class,
                () -> reactiveBuilder.forOne(reader -> Mono.error(new RuntimeException("Oops"))).block());

        verify(coreConsumer).closeAsync();
    }

    @Test
    void closesCoreConsumerAfterASubscriptionToMonoReturnedByForOneIsCancelled() {
        when(coreBuilder.subscribeAsync()).thenReturn(completedFuture(coreConsumer));
        when(coreConsumer.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = reactiveBuilder.forOne(reader -> Mono.just("a")).subscribe();
        disposable.dispose();

        verify(coreConsumer).closeAsync();
    }

    @Test
    void forManyReturnsResultOfInternalTransformation() {
        when(coreBuilder.subscribeAsync()).thenReturn(completedFuture(coreConsumer));
        when(coreConsumer.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forMany(reader -> Flux.just("a"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();
    }

    @Test
    void closesCoreConsumerAfterASubscriptionToFluxReturnedByForManyCompletesNormally() {
        when(coreBuilder.subscribeAsync()).thenReturn(completedFuture(coreConsumer));
        when(coreConsumer.closeAsync()).thenReturn(completedFuture(null));

        reactiveBuilder.forMany(reader -> Flux.just("a")).blockLast();

        verify(coreConsumer).closeAsync();
    }

    @Test
    void closesCoreConsumerAfterASubscriptionToFluxReturnedByForManyCompletesWithError() {
        when(coreBuilder.subscribeAsync()).thenReturn(completedFuture(coreConsumer));
        when(coreConsumer.closeAsync()).thenReturn(completedFuture(null));

        assertThrows(RuntimeException.class,
                () -> reactiveBuilder.forMany(reader -> Flux.error(new RuntimeException("Oops"))).blockLast());

        verify(coreConsumer).closeAsync();
    }

    @Test
    void closesCoreConsumerAfterASubscriptionToFluxReturnedByForManyIsCancelled() {
        when(coreBuilder.subscribeAsync()).thenReturn(completedFuture(coreConsumer));
        when(coreConsumer.closeAsync()).thenReturn(completedFuture(null));

        Disposable disposable = reactiveBuilder.forMany(reader -> Flux.just("a")).subscribe();
        disposable.dispose();

        verify(coreConsumer).closeAsync();
    }

    @Test
    void cloneMakesAnObject() {
        ReactiveConsumerBuilder<String> clone = reactiveBuilder.clone();

        assertThat(clone, instanceOf(ReactiveConsumerBuilder.class));
    }

    @Test
    void cloneCreatesANewObject() {
        ReactiveConsumerBuilder<String> clone = reactiveBuilder.clone();

        assertThat(clone, not(sameInstance(reactiveBuilder)));
    }

    @Test
    void invokesLoadConfOnCoreBuilder() {
        reactiveBuilder.loadConf(emptyMap());

        verify(coreBuilder).loadConf(emptyMap());
    }

    @Test
    void topicSetsTopicOnCoreBuilder() {
        reactiveBuilder.topic("topic");

        verify(coreBuilder).topic("topic");
    }

    @Test
    void topicsSetsTopicsOnCoreBuilder() {
        reactiveBuilder.topics(singletonList("topic"));

        verify(coreBuilder).topics(singletonList("topic"));
    }

    @Test
    void topicsPatternSetsTopicsPatternOnCoreBuilder() {
        Pattern pattern = Pattern.compile("topic*");
        
        reactiveBuilder.topicsPattern(pattern);

        verify(coreBuilder).topicsPattern(pattern);
    }

    @Test
    void topicsPatternStringSetsTopicsPatternOnCoreBuilder() {
        reactiveBuilder.topicsPattern("topic*");

        verify(coreBuilder).topicsPattern("topic*");
    }

    @Test
    void subscriptionNameSetsSubscriptionNameOnCoreBuilder() {
        reactiveBuilder.subscriptionName("name");

        verify(coreBuilder).subscriptionName("name");
    }

    @Test
    void setsAckTimeoutOnCoreBuilder() {
        reactiveBuilder.ackTimeout(1, TimeUnit.SECONDS);

        verify(coreBuilder).ackTimeout(1, TimeUnit.SECONDS);
    }

    @Test
    void setsAckTimeoutTickTimeOnCoreBuilder() {
        reactiveBuilder.ackTimeoutTickTime(1, TimeUnit.SECONDS);

        verify(coreBuilder).ackTimeoutTickTime(1, TimeUnit.SECONDS);
    }

    @Test
    void setsNegativeAckRedeliveryDelayOnCoreBuilder() {
        reactiveBuilder.negativeAckRedeliveryDelay(1, TimeUnit.SECONDS);

        verify(coreBuilder).negativeAckRedeliveryDelay(1, TimeUnit.SECONDS);
    }

    @Test
    void setsSubscriptionTypeOnCoreBuilder() {
        reactiveBuilder.subscriptionType(SubscriptionType.Exclusive);

        verify(coreBuilder).subscriptionType(SubscriptionType.Exclusive);
    }

    @Test
    void setsSubscriptionModeOnCoreBuilder() {
        reactiveBuilder.subscriptionMode(SubscriptionMode.Durable);

        verify(coreBuilder).subscriptionMode(SubscriptionMode.Durable);
    }

    @Test
    void setsMessageListenerOnCoreBuilder() {
        @SuppressWarnings("unchecked")
        MessageListener<String> messageListener = mock(MessageListener.class);

        reactiveBuilder.messageListener(messageListener);

        verify(coreBuilder).messageListener(messageListener);
    }

    @Test
    void setsCryptoKeyReaderOnCoreBuilder() {
        CryptoKeyReader cryptoKeyReader = mock(CryptoKeyReader.class);

        reactiveBuilder.cryptoKeyReader(cryptoKeyReader);

        verify(coreBuilder).cryptoKeyReader(cryptoKeyReader);
    }

    @Test
    void setsMessageCryptoOnCoreBuilder() {
        @SuppressWarnings("rawtypes")
        MessageCrypto messageCrypto = mock(MessageCrypto.class);

        reactiveBuilder.messageCrypto(messageCrypto);

        verify(coreBuilder).messageCrypto(messageCrypto);
    }

    @Test
    void setsCryptoFailureActionOnCoreBuilder() {
        reactiveBuilder.cryptoFailureAction(ConsumerCryptoFailureAction.FAIL);

        verify(coreBuilder).cryptoFailureAction(ConsumerCryptoFailureAction.FAIL);
    }

    @Test
    void setsCryptoFailureActionsOnCoreBuilder() {
        reactiveBuilder.cryptoFailureAction(ConsumerCryptoFailureAction.FAIL);

        verify(coreBuilder).cryptoFailureAction(ConsumerCryptoFailureAction.FAIL);
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
    void setsAcknowledgementGroupTimeOnCoreBuilder() {
        reactiveBuilder.acknowledgmentGroupTime(1, TimeUnit.SECONDS);

        verify(coreBuilder).acknowledgmentGroupTime(1, TimeUnit.SECONDS);
    }

    @Test
    void acknowledgementGroupTimeReturnsSameBuilder() {
        assertThat(reactiveBuilder.acknowledgmentGroupTime(123, TimeUnit.SECONDS), sameInstance(reactiveBuilder));
    }

    @Test
    void setsReplicateSubscriptionStateTimeOnCoreBuilder() {
        reactiveBuilder.replicateSubscriptionState(true);

        verify(coreBuilder).replicateSubscriptionState(true);
    }

    @Test
    void replicateSubscriptionStateReturnsSameBuilder() {
        assertThat(reactiveBuilder.replicateSubscriptionState(true), sameInstance(reactiveBuilder));
    }

    @Test
    void setsMaxTotalReceiverQueueSizeAcrossPartitionsOnCoreBuilder() {
        reactiveBuilder.maxTotalReceiverQueueSizeAcrossPartitions(1);

        verify(coreBuilder).maxTotalReceiverQueueSizeAcrossPartitions(1);
    }

    @Test
    void maxTotalReceiverQueueSizeAcrossPartitions() {
        assertThat(reactiveBuilder.maxTotalReceiverQueueSizeAcrossPartitions(1), sameInstance(reactiveBuilder));
    }

    @Test
    void setsConsumerNameOnCoreBuilder() {
        reactiveBuilder.consumerName("name");

        verify(coreBuilder).consumerName("name");
    }

    @Test
    void consumerNameReturnsSameBuilder() {
        assertThat(reactiveBuilder.consumerName("test"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsConsumerEventListenerOnCoreBuilder() {
        ConsumerEventListener listener = mock(ConsumerEventListener.class);

        reactiveBuilder.consumerEventListener(listener);

        verify(coreBuilder).consumerEventListener(listener);
    }

    @Test
    void consumerEventListenerReturnsSameBuilder() {
        assertThat(reactiveBuilder.consumerEventListener(mock(ConsumerEventListener.class)),
                sameInstance(reactiveBuilder));
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
    void setsPatternAutoDiscoveryPeriodMinutesOnCoreBuilder() {
        reactiveBuilder.patternAutoDiscoveryPeriod(1);

        verify(coreBuilder).patternAutoDiscoveryPeriod(1);
    }

    @Test
    void patternAutoDiscoveryPeriodMinutesReturnsSameBuilder() {
        assertThat(reactiveBuilder.patternAutoDiscoveryPeriod(1), sameInstance(reactiveBuilder));
    }

    @Test
    void setsPatternAutoDiscoveryPeriodOnCoreBuilder() {
        reactiveBuilder.patternAutoDiscoveryPeriod(1, TimeUnit.SECONDS);

        verify(coreBuilder).patternAutoDiscoveryPeriod(1, TimeUnit.SECONDS);
    }

    @Test
    void patternAutoDiscoveryPeriodReturnsSameBuilder() {
        assertThat(reactiveBuilder.patternAutoDiscoveryPeriod(1, TimeUnit.SECONDS), sameInstance(reactiveBuilder));
    }

    @Test
    void setsPriorityLevelOnCoreBuilder() {
        reactiveBuilder.priorityLevel(1);

        verify(coreBuilder).priorityLevel(1);
    }

    @Test
    void priorityLevelReturnsSameBuilder() {
        assertThat(reactiveBuilder.priorityLevel(1), sameInstance(reactiveBuilder));
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
    void setsSubscriptionInitialPositionOnCoreBuilder() {
        reactiveBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

        verify(coreBuilder).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
    }

    @Test
    void subscriptionInitialPositionReturnsSameBuilder() {
        assertThat(reactiveBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest),
                sameInstance(reactiveBuilder));
    }

    @Test
    void setsSubscriptionTopicsModeOnCoreBuilder() {
        reactiveBuilder.subscriptionTopicsMode(RegexSubscriptionMode.AllTopics);

        verify(coreBuilder).subscriptionTopicsMode(RegexSubscriptionMode.AllTopics);
    }

    @Test
    void subscriptionTopicsModeReturnsSameBuilder() {
        assertThat(reactiveBuilder.subscriptionTopicsMode(RegexSubscriptionMode.AllTopics),
                sameInstance(reactiveBuilder));
    }

    @SuppressWarnings("unchecked")
    @Test
    void invokesInterceptOnCoreBuilder() {
        ConsumerInterceptor<String> interceptor = mock(ConsumerInterceptor.class);

        reactiveBuilder.intercept(interceptor);

        verify(coreBuilder).intercept(interceptor);
    }

    @Test
    void interceptReturnsSameBuilder() {
        assertThat(reactiveBuilder.intercept(mock(ConsumerInterceptor.class)), sameInstance(reactiveBuilder));
    }

    @Test
    void setsDeadLetterPolicyOnCoreBuilder() {
        DeadLetterPolicy policy = mock(DeadLetterPolicy.class);

        reactiveBuilder.deadLetterPolicy(policy);

        verify(coreBuilder).deadLetterPolicy(policy);
    }

    @Test
    void deadLetterPolicyReturnsSameBuilder() {
        assertThat(reactiveBuilder.deadLetterPolicy(mock(DeadLetterPolicy.class)), sameInstance(reactiveBuilder));
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
    void setsKeySharedPolicyOnCoreBuilder() {
        KeySharedPolicy policy = mock(KeySharedPolicy.class);

        reactiveBuilder.keySharedPolicy(policy);

        verify(coreBuilder).keySharedPolicy(policy);
    }

    @Test
    void keySharedPolicyReturnsSameBuilder() {
        assertThat(reactiveBuilder.keySharedPolicy(mock(KeySharedPolicy.class)), sameInstance(reactiveBuilder));
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
    void setsBatchReceivePolicyOnCoreBuilder() {
        BatchReceivePolicy policy = mock(BatchReceivePolicy.class);

        reactiveBuilder.batchReceivePolicy(policy);

        verify(coreBuilder).batchReceivePolicy(policy);
    }

    @Test
    void batchReceivePolicyReturnsSameBuilder() {
        assertThat(reactiveBuilder.batchReceivePolicy(mock(BatchReceivePolicy.class)), sameInstance(reactiveBuilder));
    }

    @Test
    void setsEnableRetryOnCoreBuilder() {
        reactiveBuilder.enableRetry(true);

        verify(coreBuilder).enableRetry(true);
    }

    @Test
    void enableRetryReturnsSameBuilder() {
        assertThat(reactiveBuilder.enableRetry(true), sameInstance(reactiveBuilder));
    }

    @Test
    void setsEnableBatchIndexAcknowledgmentOnCoreBuilder() {
        reactiveBuilder.enableBatchIndexAcknowledgment(true);

        verify(coreBuilder).enableBatchIndexAcknowledgment(true);
    }

    @Test
    void enableBatchIndexAcknowledgmentReturnsSameBuilder() {
        assertThat(reactiveBuilder.enableBatchIndexAcknowledgment(true), sameInstance(reactiveBuilder));
    }

    @Test
    void setsMaxPendingChuckedMessageOnCoreBuilder() {
        reactiveBuilder.maxPendingChuckedMessage(1);

        verify(coreBuilder).maxPendingChuckedMessage(1);
    }

    @Test
    void maxPendingChuckedMessageReturnsSameBuilder() {
        assertThat(reactiveBuilder.maxPendingChuckedMessage(1), sameInstance(reactiveBuilder));
    }

    @Test
    void setsAutoAckOldestChunkedMessageOnQueueFullOnCoreBuilder() {
        reactiveBuilder.autoAckOldestChunkedMessageOnQueueFull(true);

        verify(coreBuilder).autoAckOldestChunkedMessageOnQueueFull(true);
    }

    @Test
    void autoAckOldestChunkedMessageOnQueueFullReturnsSameBuilder() {
        assertThat(reactiveBuilder.autoAckOldestChunkedMessageOnQueueFull(true), sameInstance(reactiveBuilder));
    }

    @Test
    void setsExpireTimeOfIncompleteChunkedMessageOnCoreBuilder() {
        reactiveBuilder.expireTimeOfIncompleteChunkedMessage(1, TimeUnit.SECONDS);

        verify(coreBuilder).expireTimeOfIncompleteChunkedMessage(1, TimeUnit.SECONDS);
    }

    @Test
    void expireTimeOfIncompleteChunkedMessageReturnsSameBuilder() {
        assertThat(reactiveBuilder.expireTimeOfIncompleteChunkedMessage(1, TimeUnit.SECONDS),
                sameInstance(reactiveBuilder));
    }
}