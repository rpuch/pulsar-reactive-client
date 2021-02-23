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
package com.rpuch.pulsar.reactor.impl;

import com.rpuch.pulsar.reactor.api.ReactiveReader;
import com.rpuch.pulsar.reactor.reactor.ChainStream;
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
    public Flux<Message<T>> messages() {
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
