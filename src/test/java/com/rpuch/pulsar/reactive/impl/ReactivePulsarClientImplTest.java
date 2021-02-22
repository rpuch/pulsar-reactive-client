package com.rpuch.pulsar.reactive.impl;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactivePulsarClientImplTest {
    @InjectMocks
    private ReactivePulsarClientImpl pulsarClient;

    @Mock
    private PulsarClient coreClient;

    @Test
    void closesCoreClientOnClose() throws Exception {
        pulsarClient.close();

        verify(coreClient).close();
    }
}