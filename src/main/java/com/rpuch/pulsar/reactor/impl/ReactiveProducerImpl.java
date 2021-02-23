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

import com.rpuch.pulsar.reactor.api.ReactiveProducer;
import com.rpuch.pulsar.reactor.api.ReactiveTypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Mono;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveProducerImpl<T> implements ReactiveProducer<T> {
    private final Producer<T> coreProducer;

    public ReactiveProducerImpl(Producer<T> coreProducer) {
        this.coreProducer = coreProducer;
    }

    @Override
    public String getTopic() {
        return coreProducer.getTopic();
    }

    @Override
    public String getProducerName() {
        return coreProducer.getProducerName();
    }

    @Override
    public Mono<MessageId> send(T message) {
        return Mono.fromFuture(() -> coreProducer.sendAsync(message));
    }

    @Override
    public Mono<Void> flush() {
        return Mono.fromFuture(coreProducer::flushAsync);
    }

    @Override
    public ReactiveTypedMessageBuilder<T> newMessage() {
        return new ReactiveTypedMessageBuilderImpl<>(coreProducer.newMessage());
    }

    @Override
    public <V> ReactiveTypedMessageBuilder<V> newMessage(Schema<V> schema) {
        return new ReactiveTypedMessageBuilderImpl<>(coreProducer.newMessage(schema));
    }

    @Override
    public long getLastSequenceId() {
        return coreProducer.getLastSequenceId();
    }

    @Override
    public ProducerStats getStats() {
        return coreProducer.getStats();
    }

    @Override
    public boolean isConnected() {
        return coreProducer.isConnected();
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return coreProducer.getLastDisconnectedTimestamp();
    }
}
