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
package com.rpuch.pulsar.reactor.api;

import com.rpuch.pulsar.reactor.impl.ReactivePulsarClientImpl;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.List;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactivePulsarClient extends Closeable {
    static ReactivePulsarClient from(PulsarClient coreClient) {
        return new ReactivePulsarClientImpl(coreClient);
    }

    /**
     * Create a producer builder that can be used to configure
     * and construct a producer with default {@link Schema#BYTES}.
     *
     * @return a {@link ReactiveProducerBuilder} object to configure and construct the {@link ReactiveProducer} instance
     */
    ReactiveProducerBuilder<byte[]> newProducer();

    /**
     * Create a producer builder that can be used to configure
     * and construct a producer with the specified schema.
     *
     * @param schema
     *          provide a way to convert between serialized data and domain objects
     *
     * @return a {@link ReactiveProducerBuilder} object to configure and construct the {@link ReactiveProducer} instance
     */
    <T> ReactiveProducerBuilder<T> newProducer(Schema<T> schema);

    ReactiveReaderBuilder<byte[]> newReader();

    <T> ReactiveReaderBuilder<T> newReader(Schema<T> schema);

    Mono<List<String>> getPartitionsForTopic(String topic);

    void close() throws PulsarClientException;

    Mono<Void> closeReactively();

    void shutdown() throws PulsarClientException;

    boolean isClosed();
}
