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
import com.rpuch.pulsar.reactor.reactor.ChainStream;
import com.rpuch.pulsar.reactor.reactor.Reactor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveConsumerImpl<T> implements ReactiveConsumer<T> {
    private final Consumer<T> coreConsumer;

    public ReactiveConsumerImpl(Consumer<T> coreConsumer) {
        this.coreConsumer = coreConsumer;
    }

    @Override
    public String getTopic() {
        return coreConsumer.getTopic();
    }

    @Override
    public String getSubscription() {
        return coreConsumer.getSubscription();
    }

    @Override
    public Mono<Void> unsubscribe() {
        return Reactor.FromFutureWithCancellationPropagation(coreConsumer::unsubscribeAsync);
    }

    @Override
    public Flux<Message<T>> messages() {
        return ChainStream.infiniteChain(coreConsumer::receiveAsync);
    }

    @Override
    public Mono<Message<T>> receive() {
        return Reactor.FromFutureWithCancellationPropagation(coreConsumer::receiveAsync);
    }

    @Override
    public Mono<Messages<T>> batchReceive() {
        return Reactor.FromFutureWithCancellationPropagation(coreConsumer::batchReceiveAsync);
    }

    @Override
    public Mono<Void> acknowledge(Message<?> message) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.acknowledgeAsync(message));
    }

    @Override
    public Mono<Void> acknowledge(MessageId messageId) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.acknowledgeAsync(messageId));
    }

    @Override
    public Mono<Void> acknowledge(Messages<?> messages) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.acknowledgeAsync(messages));
    }

    @Override
    public Mono<Void> acknowledge(List<MessageId> messageIdList) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.acknowledgeAsync(messageIdList));
    }

    @Override
    public void negativeAcknowledge(Message<?> message) {
        coreConsumer.negativeAcknowledge(message);
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        coreConsumer.negativeAcknowledge(messageId);
    }

    @Override
    public void negativeAcknowledge(Messages<?> messages) {
        coreConsumer.negativeAcknowledge(messages);
    }

    @Override
    public Mono<Void> reconsumeLater(Message<?> message, long delayTime, TimeUnit unit) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.reconsumeLaterAsync(message, delayTime, unit));
    }

    @Override
    public Mono<Void> reconsumeLater(Messages<?> messages, long delayTime, TimeUnit unit) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.reconsumeLaterAsync(messages, delayTime, unit));
    }

    @Override
    public Mono<Void> acknowledgeCumulative(Message<?> message) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.acknowledgeCumulativeAsync(message));
    }

    @Override
    public Mono<Void> acknowledgeCumulative(MessageId messageId) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.acknowledgeCumulativeAsync(messageId));
    }

    @Override
    public Mono<Void> reconsumeLaterCumulative(Message<?> message, long delayTime, TimeUnit unit) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.reconsumeLaterCumulativeAsync(message, delayTime, unit));
    }

    @Override
    public ConsumerStats getStats() {
        return coreConsumer.getStats();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return coreConsumer.hasReachedEndOfTopic();
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        coreConsumer.redeliverUnacknowledgedMessages();
    }

    @Override
    public Mono<Void> seek(MessageId messageId) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.seekAsync(messageId));
    }

    @Override
    public Mono<Void> seek(long timestamp) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreConsumer.seekAsync(timestamp));
    }

    @Override
    public Mono<MessageId> getLastMessageId() {
        return Reactor.FromFutureWithCancellationPropagation(coreConsumer::getLastMessageIdAsync);
    }

    @Override
    public boolean isConnected() {
        return coreConsumer.isConnected();
    }

    @Override
    public String getConsumerName() {
        return coreConsumer.getConsumerName();
    }

    @Override
    public void pause() {
        coreConsumer.pause();
    }

    @Override
    public void resume() {
        coreConsumer.resume();
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return coreConsumer.getLastDisconnectedTimestamp();
    }
}
