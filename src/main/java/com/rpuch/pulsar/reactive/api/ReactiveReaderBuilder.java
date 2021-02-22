package com.rpuch.pulsar.reactive.api;

import com.rpuch.pulsar.reactive.impl.ReactiveReaderBuilderImpl;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.ReaderBuilder;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactiveReaderBuilder<T> extends Cloneable {
    static <T> ReactiveReaderBuilder<T> from(ReaderBuilder<T> coreBuilder) {
        return new ReactiveReaderBuilderImpl<>(coreBuilder);
    }

    Flux<Message<T>> receive();

    ReactiveReaderBuilder<T> loadConf(Map<String, Object> config);

    ReactiveReaderBuilder<T> clone();

    ReactiveReaderBuilder<T> topic(String topicName);

    ReactiveReaderBuilder<T> startMessageId(MessageId startMessageId);

    ReactiveReaderBuilder<T> startMessageFromRollbackDuration(long rollbackDuration, TimeUnit timeunit);

    ReactiveReaderBuilder<T> startMessageIdInclusive();

    ReactiveReaderBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    ReactiveReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);

    ReactiveReaderBuilder<T> receiverQueueSize(int receiverQueueSize);

    ReactiveReaderBuilder<T> readerName(String readerName);

    ReactiveReaderBuilder<T> subscriptionRolePrefix(String subscriptionRolePrefix);

    ReactiveReaderBuilder<T> readCompacted(boolean readCompacted);

    ReactiveReaderBuilder<T> keyHashRange(Range... ranges);
}
