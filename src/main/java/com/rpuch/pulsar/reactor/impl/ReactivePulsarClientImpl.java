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

import com.rpuch.pulsar.reactor.api.ReactiveConsumerBuilder;
import com.rpuch.pulsar.reactor.api.ReactiveProducerBuilder;
import com.rpuch.pulsar.reactor.api.ReactivePulsarClient;
import com.rpuch.pulsar.reactor.api.ReactiveReaderBuilder;
import com.rpuch.pulsar.reactor.reactor.Reactor;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * @author Roman Puchkovskiy
 */
public class ReactivePulsarClientImpl implements ReactivePulsarClient {
    private final PulsarClient coreClient;

    public ReactivePulsarClientImpl(PulsarClient coreClient) {
        this.coreClient = coreClient;
    }

    @Override
    public ReactiveProducerBuilder<byte[]> newProducer() {
        return new ReactiveProducerBuilderImpl<>(coreClient.newProducer());
    }

    @Override
    public <T> ReactiveProducerBuilder<T> newProducer(Schema<T> schema) {
        return new ReactiveProducerBuilderImpl<>(coreClient.newProducer(schema));
    }

    @Override
    public ReactiveConsumerBuilder<byte[]> newConsumer() {
        return new ReactiveConsumerBuilderImpl<>(coreClient.newConsumer());
    }

    @Override
    public <T> ReactiveConsumerBuilder<T> newConsumer(Schema<T> schema) {
        return new ReactiveConsumerBuilderImpl<>(coreClient.newConsumer(schema));
    }

    @Override
    public ReactiveReaderBuilder<byte[]> newReader() {
        return new ReactiveReaderBuilderImpl<>(coreClient.newReader());
    }

    @Override
    public <T> ReactiveReaderBuilder<T> newReader(Schema<T> schema) {
        return new ReactiveReaderBuilderImpl<>(coreClient.newReader(schema));
    }

    @Override
    public Mono<List<String>> getPartitionsForTopic(String topic) {
        return Reactor.FromFutureWithCancellationPropagation(() -> coreClient.getPartitionsForTopic(topic));
    }

    @Override
    public void close() throws PulsarClientException {
        coreClient.close();
    }

    @Override
    public Mono<Void> closeReactively() {
        return PulsarClientClosure.closeQuietly(coreClient::closeAsync);
    }

    @Override
    public void shutdown() throws PulsarClientException {
        coreClient.shutdown();
    }

    @Override
    public boolean isClosed() {
        return coreClient.isClosed();
    }
}
