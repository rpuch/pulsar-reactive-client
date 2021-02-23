package com.rpuch.pulsar.reactor.utils;

/**
 * @author rpuch
 */
public class PollTimedOutException extends RuntimeException {
    public PollTimedOutException(String message) {
        super(message);
    }
}
