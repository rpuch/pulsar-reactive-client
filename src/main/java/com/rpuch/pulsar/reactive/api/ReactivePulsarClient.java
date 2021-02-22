package com.rpuch.pulsar.reactive.api;

import com.rpuch.pulsar.reactive.impl.ReactivePulsarClientImpl;
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

    ReactiveReaderBuilder<byte[]> newReader();

    <T> ReactiveReaderBuilder<T> newReader(Schema<T> schema);

    Mono<List<String>> getPartitionsForTopic(String topic);

    void close() throws PulsarClientException;

    Mono<Void> closeReactively();

    void shutdown() throws PulsarClientException;

    boolean isClosed();
}
