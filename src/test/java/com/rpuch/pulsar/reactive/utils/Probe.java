package com.rpuch.pulsar.reactive.utils;

/**
 * @author Roman Puchkovskiy
 */
public interface Probe<T> {
    T sample();

    boolean isFinished(T value);
}
