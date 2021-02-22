package com.rpuch.pulsar.reactive.api;

import com.rpuch.pulsar.reactive.impl.ReactiveReaderBuilderImpl;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ReaderBuilder;
import reactor.core.publisher.Flux;

import java.util.concurrent.TimeUnit;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactiveReaderBuilder<T> {
    static <T> ReactiveReaderBuilder<T> from(ReaderBuilder<T> coreBuilder) {
        return new ReactiveReaderBuilderImpl<>(coreBuilder);
    }

    ReactiveReaderBuilder<T> topic(String topicName);

    ReactiveReaderBuilder<T> startMessageId(MessageId startMessageId);

    ReactiveReaderBuilder<T> startMessageFromRollbackDuration(long rollbackDuration, TimeUnit timeunit);

    ReactiveReaderBuilder<T> startMessageIdInclusive();

    Flux<Message<T>> receive();
}
