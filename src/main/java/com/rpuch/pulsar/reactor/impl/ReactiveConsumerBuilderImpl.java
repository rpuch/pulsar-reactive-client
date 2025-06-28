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

import com.rpuch.pulsar.reactor.api.ReactiveConsumer;
import com.rpuch.pulsar.reactor.api.ReactiveConsumerBuilder;
import com.rpuch.pulsar.reactor.reactor.Reactor;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveConsumerBuilderImpl<T> implements ReactiveConsumerBuilder<T> {
    private final ConsumerBuilder<T> coreBuilder;

    public ReactiveConsumerBuilderImpl(ConsumerBuilder<T> coreBuilder) {
        this.coreBuilder = coreBuilder;
    }

    @Override
    public <U> Mono<U> forOne(Function<? super ReactiveConsumer<T>, ? extends Mono<U>> transformation) {
        return Mono.usingWhen(
                createCoreConsumer(),
                coreConsumer -> transformation.apply(new ReactiveConsumerImpl<>(coreConsumer)),
                this::closeQuietly
        );
    }

    private Mono<Void> closeQuietly(Consumer<T> coreConsumer) {
        return PulsarClientClosure.closeQuietly(coreConsumer::closeAsync);
    }

    @Override
    public <U> Flux<U> forMany(Function<? super ReactiveConsumer<T>, ? extends Flux<U>> transformation) {
        return Flux.usingWhen(
                createCoreConsumer(),
                coreConsumer -> transformation.apply(new ReactiveConsumerImpl<>(coreConsumer)),
                this::closeQuietly
        );
    }

    private Mono<Consumer<T>> createCoreConsumer() {
        return Reactor.fromFutureWithCancellationPropagation(coreBuilder::subscribeAsync);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public ReactiveConsumerBuilder<T> clone() {
        return new ReactiveConsumerBuilderImpl<>(coreBuilder.clone());
    }

    @Override
    public ReactiveConsumerBuilder<T> loadConf(Map<String, Object> config) {
        coreBuilder.loadConf(config);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> topic(String... topicNames) {
        coreBuilder.topic(topicNames);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> topics(List<String> topicNames) {
        coreBuilder.topics(topicNames);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> topicsPattern(Pattern topicsPattern) {
        coreBuilder.topicsPattern(topicsPattern);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> topicsPattern(String topicsPattern) {
        coreBuilder.topicsPattern(topicsPattern);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> subscriptionName(String subscriptionName) {
        coreBuilder.subscriptionName(subscriptionName);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> ackTimeout(long ackTimeout, TimeUnit timeUnit) {
        coreBuilder.ackTimeout(ackTimeout, timeUnit);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> ackTimeoutTickTime(long tickTime, TimeUnit timeUnit) {
        coreBuilder.ackTimeoutTickTime(tickTime, timeUnit);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> negativeAckRedeliveryDelay(long redeliveryDelay, TimeUnit timeUnit) {
        coreBuilder.negativeAckRedeliveryDelay(redeliveryDelay, timeUnit);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> subscriptionType(SubscriptionType subscriptionType) {
        coreBuilder.subscriptionType(subscriptionType);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> subscriptionMode(SubscriptionMode subscriptionMode) {
        coreBuilder.subscriptionMode(subscriptionMode);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> messageListener(MessageListener<T> messageListener) {
        coreBuilder.messageListener(messageListener);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        coreBuilder.cryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ReactiveConsumerBuilder<T> messageCrypto(MessageCrypto messageCrypto) {
        coreBuilder.messageCrypto(messageCrypto);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action) {
        coreBuilder.cryptoFailureAction(action);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> receiverQueueSize(int receiverQueueSize) {
        coreBuilder.receiverQueueSize(receiverQueueSize);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit) {
        coreBuilder.acknowledgmentGroupTime(delay, unit);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> replicateSubscriptionState(boolean replicateSubscriptionState) {
        coreBuilder.replicateSubscriptionState(replicateSubscriptionState);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> maxTotalReceiverQueueSizeAcrossPartitions(
            int maxTotalReceiverQueueSizeAcrossPartitions) {
        coreBuilder.maxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> consumerName(String consumerName) {
        coreBuilder.consumerName(consumerName);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> consumerEventListener(ConsumerEventListener consumerEventListener) {
        coreBuilder.consumerEventListener(consumerEventListener);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> readCompacted(boolean readCompacted) {
        coreBuilder.readCompacted(readCompacted);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> patternAutoDiscoveryPeriod(int periodInMinutes) {
        coreBuilder.patternAutoDiscoveryPeriod(periodInMinutes);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> patternAutoDiscoveryPeriod(int interval, TimeUnit unit) {
        coreBuilder.patternAutoDiscoveryPeriod(interval, unit);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> priorityLevel(int priorityLevel) {
        coreBuilder.priorityLevel(priorityLevel);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> property(String key, String value) {
        coreBuilder.property(key, value);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> properties(Map<String, String> properties) {
        coreBuilder.properties(properties);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> subscriptionInitialPosition(
            SubscriptionInitialPosition subscriptionInitialPosition) {
        coreBuilder.subscriptionInitialPosition(subscriptionInitialPosition);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> subscriptionTopicsMode(RegexSubscriptionMode regexSubscriptionMode) {
        coreBuilder.subscriptionTopicsMode(regexSubscriptionMode);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ReactiveConsumerBuilder<T> intercept(ConsumerInterceptor<T>... interceptors) {
        coreBuilder.intercept(interceptors);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy) {
        coreBuilder.deadLetterPolicy(deadLetterPolicy);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> autoUpdatePartitions(boolean autoUpdate) {
        coreBuilder.autoUpdatePartitions(autoUpdate);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit) {
        coreBuilder.autoUpdatePartitionsInterval(interval, unit);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> keySharedPolicy(KeySharedPolicy keySharedPolicy) {
        coreBuilder.keySharedPolicy(keySharedPolicy);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> startMessageIdInclusive() {
        coreBuilder.startMessageIdInclusive();
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> batchReceivePolicy(BatchReceivePolicy batchReceivePolicy) {
        coreBuilder.batchReceivePolicy(batchReceivePolicy);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> enableRetry(boolean retryEnable) {
        coreBuilder.enableRetry(retryEnable);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> enableBatchIndexAcknowledgment(boolean batchIndexAcknowledgmentEnabled) {
        coreBuilder.enableBatchIndexAcknowledgment(batchIndexAcknowledgmentEnabled);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> maxPendingChunkedMessage(int maxPendingChunkedMessage) {
        coreBuilder.maxPendingChunkedMessage(maxPendingChunkedMessage);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> autoAckOldestChunkedMessageOnQueueFull(
            boolean autoAckOldestChunkedMessageOnQueueFull) {
        coreBuilder.autoAckOldestChunkedMessageOnQueueFull(autoAckOldestChunkedMessageOnQueueFull);
        return this;
    }

    @Override
    public ReactiveConsumerBuilder<T> expireTimeOfIncompleteChunkedMessage(long duration, TimeUnit unit) {
        coreBuilder.expireTimeOfIncompleteChunkedMessage(duration, unit);
        return this;
    }
}
