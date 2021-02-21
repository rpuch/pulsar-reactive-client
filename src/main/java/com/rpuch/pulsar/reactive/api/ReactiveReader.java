package com.rpuch.pulsar.reactive.api;

import org.apache.pulsar.client.api.Message;
import reactor.core.publisher.Flux;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactiveReader<T> {
    Flux<Message<T>> receive();
}
