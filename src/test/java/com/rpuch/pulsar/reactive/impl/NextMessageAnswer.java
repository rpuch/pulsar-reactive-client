package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.utils.Futures;
import org.apache.pulsar.client.api.Message;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

/**
 * @author Roman Puchkovskiy
 */
class NextMessageAnswer implements Answer<CompletableFuture<Message<String>>> {
    private final List<Action> actions;
    private final AtomicInteger nextActionIndex = new AtomicInteger(0);

    NextMessageAnswer(Action... actions) {
        this.actions = new ArrayList<>(Arrays.asList(actions));
    }

    static Answer<CompletableFuture<Message<String>>> produce(String... values) {
        Action[] actions = Arrays.stream(values)
                .map(Return::new)
                .toArray(Action[]::new);
        return new NextMessageAnswer(actions);
    }

    static Answer<CompletableFuture<Message<String>>> failWith(Throwable ex) {
        return new NextMessageAnswer(new Throw(ex));
    }

    @Override
    public CompletableFuture<Message<String>> answer(InvocationOnMock invocationOnMock) {
        if (nextActionIndex.get() >= actions.size()) {
            return new CompletableFuture<>();
        }
        Action action = actions.get(nextActionIndex.getAndIncrement());
        return action.act();
    }

    interface Action {
        CompletableFuture<Message<String>> act();
    }

    static class Return implements Action {
        private final String value;

        Return(String value) {
            this.value = value;
        }

        @Override
        public CompletableFuture<Message<String>> act() {
            Message<String> message = messageReturningValue(value);
            return CompletableFuture.completedFuture(message);
        }

        private Message<String> messageReturningValue(String v) {
            @SuppressWarnings("unchecked")
            Message<String> message = mock(Message.class);
            lenient().when(message.getValue()).thenReturn(v);
            return message;
        }
    }

    static class Throw implements Action {
        private final Throwable ex;

        Throw(Throwable ex) {
            this.ex = ex;
        }

        @Override
        public CompletableFuture<Message<String>> act() {
            return Futures.failedFuture(ex);
        }
    }
}
