package com.rpuch.pulsar.reactive.reactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
        }, FluxSink.OverflowStrategy.ERROR);
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