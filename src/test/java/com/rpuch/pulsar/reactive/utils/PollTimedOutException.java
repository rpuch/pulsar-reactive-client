package com.rpuch.pulsar.reactive.utils;

/**
 * @author rpuch
 */
public class PollTimedOutException extends RuntimeException {
    public PollTimedOutException(String message) {
        super(message);
    }
}
