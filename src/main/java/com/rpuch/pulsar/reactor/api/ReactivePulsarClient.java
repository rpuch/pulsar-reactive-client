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
