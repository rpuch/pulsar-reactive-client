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
package com.rpuch.pulsar.reactor.reactor;

import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * @author Roman Puchkovskiy
 */
public class Reactor {
    public static <T> Mono<T> monoFromFuture(Supplier<? extends CompletableFuture<? extends T>> futureSupplier) {
        AtomicReference<CompletableFuture<? extends T>> futureRef = new AtomicReference<>();
        return Mono
                .fromFuture(() -> {
                    CompletableFuture<? extends T> future = futureSupplier.get();
                    futureRef.set(future);
                    return future;
                })
                .doOnCancel(() -> cancelIfNotDone(futureRef))
                .map(x -> x);
    }

    private static <T> void cancelIfNotDone(AtomicReference<CompletableFuture<? extends T>> futureRef) {
        CompletableFuture<? extends T> future = futureRef.get();
        if (future != null && !future.isDone()) {
            future.cancel(false);
        }
    }

    private Reactor() {
    }
}
