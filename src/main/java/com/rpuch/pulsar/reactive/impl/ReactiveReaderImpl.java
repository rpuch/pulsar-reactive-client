package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.api.ReactiveReader;
import com.rpuch.pulsar.reactive.reactor.ChainStream;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveReaderImpl<T> implements ReactiveReader<T> {
    private final Reader<T> reader;

    public ReactiveReaderImpl(Reader<T> reader) {
        this.reader = reader;
    }

    @Override
    public String getTopic() {
        return reader.getTopic();
    }

    @Override
    public Flux<Message<T>> receive() {
        return ChainStream.infiniteChain(reader::readNextAsync);
    }

    @Override
    public Mono<Message<T>> readNext() {
        return Mono.fromFuture(reader::readNextAsync);
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return reader.hasReachedEndOfTopic();
    }

    @Override
    public Mono<Boolean> hasMessageAvailable() {
        return Mono.fromFuture(reader::hasMessageAvailableAsync);
    }

    @Override
    public boolean isConnected() {
        return reader.isConnected();
    }

    @Override
    public Mono<Void> seek(MessageId messageId) {
        return Mono.fromFuture(() -> reader.seekAsync(messageId));
    }

    @Override
    public Mono<Void> seek(long timestamp) {
        return Mono.fromFuture(() -> reader.seekAsync(timestamp));
    }
}
