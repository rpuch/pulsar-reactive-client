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

import com.rpuch.pulsar.reactor.api.ReactiveTypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveTypedMessageBuilderImpl<T> implements ReactiveTypedMessageBuilder<T> {
    private final TypedMessageBuilder<T> coreBuilder;

    public ReactiveTypedMessageBuilderImpl(TypedMessageBuilder<T> coreBuilder) {
        this.coreBuilder = coreBuilder;
    }

    @Override
    public Mono<MessageId> send() {
        return Mono.fromFuture(coreBuilder::sendAsync);
    }

    @Override
    public ReactiveTypedMessageBuilder<T> key(String key) {
        coreBuilder.key(key);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> keyBytes(byte[] key) {
        coreBuilder.keyBytes(key);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> orderingKey(byte[] orderingKey) {
        coreBuilder.orderingKey(orderingKey);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> value(T value) {
        coreBuilder.value(value);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> property(String name, String value) {
        coreBuilder.property(name, value);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> properties(Map<String, String> properties) {
        coreBuilder.properties(properties);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> eventTime(long timestamp) {
        coreBuilder.eventTime(timestamp);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> sequenceId(long sequenceId) {
        coreBuilder.sequenceId(sequenceId);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> replicationClusters(List<String> clusters) {
        coreBuilder.replicationClusters(clusters);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> disableReplication() {
        coreBuilder.disableReplication();
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> deliverAt(long timestamp) {
        coreBuilder.deliverAt(timestamp);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
        coreBuilder.deliverAfter(delay, unit);
        return this;
    }

    @Override
    public ReactiveTypedMessageBuilder<T> loadConf(Map<String, Object> config) {
        coreBuilder.loadConf(config);
        return this;
    }
}
