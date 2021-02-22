package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.api.ReactiveReaderBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveReaderBuilderImpl<T> implements ReactiveReaderBuilder<T> {
    private final ReaderBuilder<T> coreBuilder;

    public ReactiveReaderBuilderImpl(ReaderBuilder<T> coreBuilder) {
        this.coreBuilder = coreBuilder;
    }

    @Override
    public ReactiveReaderBuilder<T> topic(String topicName) {
        coreBuilder.topic(topicName);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> startMessageId(MessageId startMessageId) {
        coreBuilder.startMessageId(startMessageId);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> startMessageFromRollbackDuration(long rollbackDuration, TimeUnit timeunit) {
        coreBuilder.startMessageFromRollbackDuration(rollbackDuration, timeunit);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> startMessageIdInclusive() {
        coreBuilder.startMessageIdInclusive();
        return this;
    }

    @Override
    public Flux<Message<T>> receive() {
        return Flux.usingWhen(
                createCoreReader(),
                coreReader -> new ReactiveReaderImpl<>(coreReader).receive(),
                coreReader -> Mono.fromFuture(coreReader::closeAsync)
        );
    }

    private Mono<Reader<T>> createCoreReader() {
        return Mono.fromFuture(coreBuilder::createAsync);
    }
}
