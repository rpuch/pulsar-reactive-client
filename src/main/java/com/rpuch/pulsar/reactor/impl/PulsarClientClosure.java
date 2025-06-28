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

import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author Roman Puchkovskiy
 */
public class PulsarClientClosure {
    public static Mono<Void> closeQuietly(Supplier<? extends CompletableFuture<Void>> closerSupplier) {
        return fromWithoutCancellationPropagation(closerSupplier)
                .onErrorResume(AlreadyClosedException.class, ex -> Mono.empty());
    }

    private static Mono<Void> fromWithoutCancellationPropagation(
            Supplier<? extends CompletableFuture<Void>> closerSupplier) {
        return Mono.fromFuture(closerSupplier);
    }

    private PulsarClientClosure() {
    }
}
