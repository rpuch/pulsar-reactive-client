package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.api.ReactiveReader;
import com.rpuch.pulsar.reactive.api.ReactiveReaderBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveReaderBuilderImpl<T> implements ReactiveReaderBuilder<T> {
    private final ReaderBuilder<T> coreBuilder;

    public ReactiveReaderBuilderImpl(ReaderBuilder<T> coreBuilder) {
        this.coreBuilder = coreBuilder;
    }

    @Override
    public ReactiveReaderBuilder<T> loadConf(Map<String, Object> config) {
        coreBuilder.loadConf(config);
        return this;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public ReactiveReaderBuilder<T> clone() {
        return new ReactiveReaderBuilderImpl<>(coreBuilder.clone());
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
    public ReactiveReaderBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        coreBuilder.cryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action) {
        coreBuilder.cryptoFailureAction(action);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> receiverQueueSize(int receiverQueueSize) {
        coreBuilder.receiverQueueSize(receiverQueueSize);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> readerName(String readerName) {
        coreBuilder.readerName(readerName);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> subscriptionRolePrefix(String subscriptionRolePrefix) {
        coreBuilder.subscriptionRolePrefix(subscriptionRolePrefix);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> readCompacted(boolean readCompacted) {
        coreBuilder.readCompacted(readCompacted);
        return this;
    }

    @Override
    public ReactiveReaderBuilder<T> keyHashRange(Range... ranges) {
        coreBuilder.keyHashRange(ranges);
        return this;
    }

    @Override
    public Flux<Message<T>> receive() {
        return forMany(ReactiveReader::receive);
    }

    @Override
    public <U> Mono<U> forOne(Function<? super ReactiveReader<T>, ? extends Mono<U>> transformation) {
        return Mono.usingWhen(
                createCoreReader(),
                coreReader -> transformation.apply(new ReactiveReaderImpl<>(coreReader)),
                coreReader -> Mono.fromFuture(coreReader::closeAsync)
        );
    }

    @Override
    public <U> Flux<U> forMany(Function<? super ReactiveReader<T>, ? extends Flux<U>> transformation) {
        return Flux.usingWhen(
                createCoreReader(),
                coreReader -> transformation.apply(new ReactiveReaderImpl<>(coreReader)),
                coreReader -> Mono.fromFuture(coreReader::closeAsync)
        );
    }

    private Mono<Reader<T>> createCoreReader() {
        return Mono.fromFuture(coreBuilder::createAsync);
    }
}
