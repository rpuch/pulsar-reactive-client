package com.rpuch.pulsar.reactor.impl;

import com.rpuch.pulsar.reactor.api.ReactiveProducerBuilder;
import com.rpuch.pulsar.reactor.api.ReactivePulsarClient;
import com.rpuch.pulsar.reactor.api.ReactiveReaderBuilder;
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
    public ReactiveReaderBuilder<byte[]> newReader() {
        return new ReactiveReaderBuilderImpl<>(coreClient.newReader());
    }

    @Override
    public <T> ReactiveReaderBuilder<T> newReader(Schema<T> schema) {
        return new ReactiveReaderBuilderImpl<>(coreClient.newReader(schema));
    }

    @Override
    public Mono<List<String>> getPartitionsForTopic(String topic) {
        return Mono.fromFuture(() -> coreClient.getPartitionsForTopic(topic));
    }

    @Override
    public void close() throws PulsarClientException {
        coreClient.close();
    }

    @Override
    public Mono<Void> closeReactively() {
        return Mono.fromFuture(coreClient::closeAsync);
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