package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.api.ReactiveProducer;
import com.rpuch.pulsar.reactive.api.ReactiveTypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Mono;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveProducerImpl<T> implements ReactiveProducer<T> {
    private final Producer<T> coreProducer;

    public ReactiveProducerImpl(Producer<T> coreProducer) {
        this.coreProducer = coreProducer;
    }

    @Override
    public String getTopic() {
        return coreProducer.getTopic();
    }

    @Override
    public String getProducerName() {
        return coreProducer.getProducerName();
    }

    @Override
    public Mono<MessageId> send(T message) {
        throw new UnsupportedOperationException("Not yet");
    }

    @Override
    public Mono<Void> flush() {
        throw new UnsupportedOperationException("Not yet");
    }

    @Override
    public ReactiveTypedMessageBuilder<T> newMessage() {
        return new ReactiveTypedMessageBuilderImpl<>(coreProducer.newMessage());
    }

    @Override
    public <V> ReactiveTypedMessageBuilder<V> newMessage(Schema<V> schema) {
        return new ReactiveTypedMessageBuilderImpl<>(coreProducer.newMessage(schema));
    }

    @Override
    public long getLastSequenceId() {
        return coreProducer.getLastSequenceId();
    }

    @Override
    public ProducerStats getStats() {
        return coreProducer.getStats();
    }

    @Override
    public boolean isConnected() {
        return coreProducer.isConnected();
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return coreProducer.getLastDisconnectedTimestamp();
    }
}
