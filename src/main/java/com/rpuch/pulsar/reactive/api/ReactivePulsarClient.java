package com.rpuch.pulsar.reactive.api;

import org.apache.pulsar.client.api.ReaderBuilder;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactivePulsarClient {
    ReaderBuilder<byte[]> newReader();
}
