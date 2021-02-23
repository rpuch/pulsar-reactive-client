package com.rpuch.pulsar.reactor.api;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactiveReader<T> {
    String getTopic();

    Flux<Message<T>> messages();

    Mono<Message<T>> readNext();

    boolean hasReachedEndOfTopic();

    Mono<Boolean> hasMessageAvailable();

    boolean isConnected();

    Mono<Void> seek(MessageId messageId);

    Mono<Void> seek(long timestamp);
}