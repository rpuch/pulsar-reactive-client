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
package com.rpuch.pulsar.reactor.impl;

import com.rpuch.pulsar.reactor.utils.Futures;
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
