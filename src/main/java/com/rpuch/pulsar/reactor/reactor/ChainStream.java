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

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ChainStream {
    public static <T> Flux<T> infiniteChain(Callable<CompletableFuture<T>> futureProvider) {
        return Flux.create(sink -> {
            Chain<T> chain = new Chain<>(futureProvider, sink);
            sink.onRequest(chain::onRequest);
            sink.onCancel(chain::onCancel);
        }, OverflowStrategy.ERROR);
    }

    private ChainStream() {
    }

    private static class Chain<T> {
        private final Callable<CompletableFuture<T>> futureProvider;
        private final FluxSink<T> sink;

        private final AtomicLong remainingDemand = new AtomicLong(0);
        private final AtomicBoolean pulling = new AtomicBoolean(false);
        private final AtomicReference<CompletableFuture<T>> currentLinkFutureRef = new AtomicReference<>();

        private Chain(Callable<CompletableFuture<T>> futureProvider, FluxSink<T> sink) {
            this.futureProvider = futureProvider;
            this.sink = sink;
        }

        public void onRequest(long demand) {
            long newDemand = remainingDemand.addAndGet(demand);

            if (newDemand < 0) {
                sink.error(new IllegalStateException("Demand after increment is negative: " + newDemand));
                return;
            }
            if (newDemand == 0) {
                return;
            }

            // demand is positive
            if (switchToReadingIfNotReading()) {
                pullNextChainLink();
            }
        }

        private boolean switchToReadingIfNotReading() {
            return pulling.compareAndSet(false, true);
        }

        private void pullNextChainLink() {
            final CompletableFuture<T> future;
            try {
                future = futureProvider.call();
            } catch (Exception e) {
                sink.error(e);
                return;
            }

            currentLinkFutureRef.set(future);

            future.whenComplete(this::onLinkReceived);
        }

        private void onLinkReceived(T element, Throwable ex) {
            if (ex != null) {
                sink.error(ex);
                return;
            }

            sink.next(element);

            final long demandAfterDecrement = remainingDemand.decrementAndGet();
            if (demandAfterDecrement < 0) {
                sink.error(new IllegalStateException("Demand after decrement is negative " + demandAfterDecrement));
            } else if (demandAfterDecrement > 0) {
                pullNextChainLink();
            } else {
                // remaining demand is zero, stop
                pulling.set(false);
            }
        }

        public void onCancel() {
            final CompletableFuture<T> future = currentLinkFutureRef.get();
            if (future != null) {
                future.cancel(true);
            }
        }
    }
}