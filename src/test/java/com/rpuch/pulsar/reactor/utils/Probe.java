package com.rpuch.pulsar.reactor.utils;

/**
 * @author Roman Puchkovskiy
 */
public interface Probe<T> {
    T sample();

    boolean isFinished(T value);
}
