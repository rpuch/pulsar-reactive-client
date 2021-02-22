package com.rpuch.pulsar.reactive.impl;

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.util.Arrays;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactivePulsarClientImplTest {
    @InjectMocks
    private ReactivePulsarClientImpl reactiveClient;

    @Mock
    private PulsarClient coreClient;

    @Test
    void closesCoreClientOnClose() throws Exception {
        reactiveClient.close();

        verify(coreClient).close();
    }

    @Test
    void closesCoreClientAsynchronouslyOnReactiveCloseSubscription() {
        when(coreClient.closeAsync()).thenReturn(completedFuture(null));

        reactiveClient.closeReactively()
                .as(StepVerifier::create)
                .verifyComplete();

        verify(coreClient).closeAsync();
    }

    @Test
    void returnsPartitionsForTopicFromGetPartitionsForTopicOnCoreClient() {
        when(coreClient.getPartitionsForTopic("topic"))
                .thenReturn(completedFuture(Arrays.asList("a", "b")));

        reactiveClient.getPartitionsForTopic("topic")
                .as(StepVerifier::create)
                .expectNext(Arrays.asList("a", "b"))
                .verifyComplete();
    }

    @Test
    void shutdownCallsShutdownOnCoreClient() throws Exception {
        reactiveClient.shutdown();

        verify(coreClient).shutdown();
    }

    @Test
    void isClosedConsultsCoreClient() {
        when(coreClient.isClosed()).thenReturn(true);

        assertTrue(reactiveClient.isClosed());

        verify(coreClient).isClosed();
    }
}