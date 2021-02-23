package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.api.ReactiveProducer;
import com.rpuch.pulsar.reactive.api.ReactiveProducerBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveProducerBuilderImpl<T> implements ReactiveProducerBuilder<T> {
    private final ProducerBuilder<T> coreBuilder;

    public ReactiveProducerBuilderImpl(ProducerBuilder<T> coreBuilder) {
        this.coreBuilder = coreBuilder;
    }

    @Override
    public <U> Mono<U> forOne(Function<? super ReactiveProducer<T>, ? extends Mono<U>> transformation) {
        return Mono.usingWhen(
                createCoreProducer(),
                coreReader -> transformation.apply(new ReactiveProducerImpl<>(coreReader)),
                coreReader -> Mono.fromFuture(coreReader::closeAsync)
        );
    }

    @Override
    public <U> Flux<U> forMany(Function<? super ReactiveProducer<T>, ? extends Flux<U>> transformation) {
        return Flux.usingWhen(
                createCoreProducer(),
                coreReader -> transformation.apply(new ReactiveProducerImpl<>(coreReader)),
                coreReader -> Mono.fromFuture(coreReader::closeAsync)
        );
    }

    private Mono<Producer<T>> createCoreProducer() {
        return Mono.fromFuture(coreBuilder::createAsync);
    }

    @Override
    public ReactiveProducerBuilder<T> loadConf(Map<String, Object> config) {
        coreBuilder.loadConf(config);
        return this;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public ReactiveProducerBuilder<T> clone() {
        return new ReactiveProducerBuilderImpl<>(coreBuilder.clone());
    }

    @Override
    public ReactiveProducerBuilder<T> topic(String topicName) {
        coreBuilder.topic(topicName);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> producerName(String producerName) {
        coreBuilder.producerName(producerName);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> sendTimeout(int sendTimeout, TimeUnit unit) {
        coreBuilder.sendTimeout(sendTimeout, unit);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> maxPendingMessages(int maxPendingMessages) {
        coreBuilder.maxPendingMessages(maxPendingMessages);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> maxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
        coreBuilder.maxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> messageRoutingMode(MessageRoutingMode messageRoutingMode) {
        coreBuilder.messageRoutingMode(messageRoutingMode);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> hashingScheme(HashingScheme hashingScheme) {
        coreBuilder.hashingScheme(hashingScheme);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> compressionType(CompressionType compressionType) {
        coreBuilder.compressionType(compressionType);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> messageRouter(MessageRouter messageRouter) {
        coreBuilder.messageRouter(messageRouter);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        coreBuilder.cryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> addEncryptionKey(String key) {
        coreBuilder.addEncryptionKey(key);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> cryptoFailureAction(ProducerCryptoFailureAction action) {
        coreBuilder.cryptoFailureAction(action);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> initialSequenceId(long initialSequenceId) {
        coreBuilder.initialSequenceId(initialSequenceId);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> property(String key, String value) {
        coreBuilder.property(key, value);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> properties(Map<String, String> properties) {
        coreBuilder.properties(properties);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> intercept(ProducerInterceptor... interceptors) {
        coreBuilder.intercept(interceptors);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> autoUpdatePartitions(boolean autoUpdate) {
        coreBuilder.autoUpdatePartitions(autoUpdate);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit) {
        coreBuilder.autoUpdatePartitionsInterval(interval, unit);
        return this;
    }

    @Override
    public ReactiveProducerBuilder<T> enableMultiSchema(boolean multiSchema) {
        coreBuilder.enableMultiSchema(multiSchema);
        return this;
    }
}
