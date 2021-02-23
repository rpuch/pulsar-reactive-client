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
