package com.rpuch.pulsar.reactive.utils;

import java.util.concurrent.CompletableFuture;

/**
 * @author Roman Puchkovskiy
 */
public class Futures {
    public static <T>CompletableFuture<T> failedFuture(Throwable ex) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }

    private Futures() {
    }
}
