/*
 * Copyright 2021 Pulsar Reactive Client contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rpuch.pulsar.reactor.api;

import com.rpuch.pulsar.reactor.impl.ReactiveReaderBuilderImpl;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.ReaderBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactiveReaderBuilder<T> extends Cloneable {
    static <T> ReactiveReaderBuilder<T> from(ReaderBuilder<T> coreBuilder) {
        return new ReactiveReaderBuilderImpl<>(coreBuilder);
    }

    Flux<Message<T>> messages();

    <U> Mono<U> forOne(Function<? super ReactiveReader<T>, ? extends Mono<U>> transformation);

    <U> Flux<U> forMany(Function<? super ReactiveReader<T>, ? extends Flux<U>> transformation);

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
