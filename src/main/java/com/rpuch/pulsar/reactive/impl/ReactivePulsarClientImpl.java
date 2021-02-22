package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.api.ReactivePulsarClient;
import com.rpuch.pulsar.reactive.api.ReactiveReaderBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/**
 * @author Roman Puchkovskiy
 */
public class ReactivePulsarClientImpl implements ReactivePulsarClient {
    private final PulsarClient coreClient;

    public ReactivePulsarClientImpl(PulsarClient coreClient) {
        this.coreClient = coreClient;
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
    public void close() throws PulsarClientException {
        coreClient.close();
    }
}
