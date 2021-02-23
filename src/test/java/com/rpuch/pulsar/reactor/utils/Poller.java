package com.rpuch.pulsar.reactor.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.time.Instant.now;

/**
 * @author rpuch
 */
public class Poller {
    private final Duration maxPollDuration;
    private final Duration sleepDuration;

    public Poller(Duration maxPollDuration) {
        this(maxPollDuration, Duration.ofMillis(100));
    }

    public Poller(Duration maxPollDuration, Duration sleepDuration) {
        this.maxPollDuration = maxPollDuration;
        this.sleepDuration = sleepDuration;
    }

    public <T> T poll(Supplier<? extends T> sampler, Predicate<? super T> finisher) throws InterruptedException {
        return poll(new CombiningProbe<T>(sampler, finisher));
    }

    public <T> T poll(String pollTimedOutMessage, Supplier<? extends T> sampler, Predicate<? super T> finisher)
            throws InterruptedException {
        return poll(pollTimedOutMessage, new CombiningProbe<T>(sampler, finisher));
    }

    public <T> T poll(Probe<T> probe) throws InterruptedException {
        return poll("Did not sample anything matching in " + maxPollDuration, probe);
    }

    public <T> T poll(String pollTimedOutMessage, Probe<T> probe) throws InterruptedException {
        Instant endTime = now().plus(maxPollDuration);

        while (now().isBefore(endTime)) {
            T value = probe.sample();
            if (probe.isFinished(value)) {
                return value;
            }

            //noinspection BusyWait
            Thread.sleep(sleepDuration.toMillis());
        }

        return handleFailedPolling(pollTimedOutMessage);
    }

    private <T> T handleFailedPolling(String pollTimedOutMessage) {
        throw new PollTimedOutException(pollTimedOutMessage);
    }

    public void pollTill(String pollTimedOutMessage, BooleanSupplier supplier) throws InterruptedException {
        poll(pollTimedOutMessage, supplier::getAsBoolean, booleanValue -> booleanValue);
    }

    private static class CombiningProbe<T> implements Probe<T> {
        private final Supplier<? extends T> sampler;
        private final Predicate<? super T> finisher;

        CombiningProbe(Supplier<? extends T> sampler, Predicate<? super T> finisher) {
            this.sampler = sampler;
            this.finisher = finisher;
        }

        @Override
        public T sample() {
            return sampler.get();
        }

        @Override
        public boolean isFinished(T value) {
            return finisher.test(value);
        }
    }
}
