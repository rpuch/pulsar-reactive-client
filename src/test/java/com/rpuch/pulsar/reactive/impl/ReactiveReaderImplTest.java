package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.utils.Futures;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Reader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveReaderImplTest {
    @InjectMocks
    private ReactiveReaderImpl<String> reactiveReader;

    @Mock
    private Reader<String> coreReader;

    @Test
    void receiveReceivesMessagesFromRepetitiveReadNextAsync() {
        when(coreReader.readNextAsync()).then(produce("a", "b"));

        reactiveReader.receive()
                .as(StepVerifier::create)
                .assertNext(message -> assertThat(message.getValue(), is("a")))
                .assertNext(message -> assertThat(message.getValue(), is("b")))
                .expectTimeout(Duration.ofMillis(100))
                .verify();
    }

    @Test
    void receiveConvertsFailureToError() {
        Exception exception = new Exception("Oops");
        when(coreReader.readNextAsync()).then(failWith(exception));

        reactiveReader.receive()
                .as(StepVerifier::create)
                .expectErrorSatisfies(ex -> assertThat(ex, sameInstance(exception)))
                .verify();
    }

    @Test
    void onlyCallsReadNextAsyncWhenDemandIsRequestedThroughFluxReturnedByReceive() {
        when(coreReader.readNextAsync()).then(produce("a", "b"));

        Flux<Message<String>> messages = reactiveReader.receive();
        requestExactlyOneMessage(messages);

        verify(coreReader, times(1)).readNextAsync();
    }

    private void requestExactlyOneMessage(Flux<Message<String>> messages) {
        messages.limitRequest(1).blockFirst();
    }

    private Answer<CompletableFuture<Message<String>>> produce(String... values) {
        Action[] actions = Arrays.stream(values)
                .map(Return::new)
                .toArray(Action[]::new);
        return new NextMessageAnswer(actions);
    }

    private Answer<CompletableFuture<Message<String>>> failWith(Throwable ex) {
        return new NextMessageAnswer(new Throw(ex));
    }

    private static class NextMessageAnswer implements Answer<CompletableFuture<Message<String>>> {
        private final List<Action> actions;
        private final AtomicInteger nextActionIndex = new AtomicInteger(0);

        private NextMessageAnswer(Action... actions) {
            this.actions = new ArrayList<>(Arrays.asList(actions));
        }

        @Override
        public CompletableFuture<Message<String>> answer(InvocationOnMock invocationOnMock) {
            if (nextActionIndex.get() >= actions.size()) {
                return new CompletableFuture<>();
            }
            Action action = actions.get(nextActionIndex.getAndIncrement());
            return action.act();
        }
    }

    private interface Action {
        CompletableFuture<Message<String>> act();
    }

    private static class Return implements Action {
        private final String value;

        private Return(String value) {
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

    private static class Throw implements Action {
        private final Throwable ex;

        private Throw(Throwable ex) {
            this.ex = ex;
        }

        @Override
        public CompletableFuture<Message<String>> act() {
            return Futures.failedFuture(ex);
        }
    }
}