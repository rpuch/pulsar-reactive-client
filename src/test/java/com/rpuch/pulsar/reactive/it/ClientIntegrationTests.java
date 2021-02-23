package com.rpuch.pulsar.reactive.it;

import com.rpuch.pulsar.reactive.api.ReactivePulsarClient;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

/**
 * @author Roman Puchkovskiy
 */
public class ClientIntegrationTests extends TestWithPulsar {
    private PulsarClient coreClient;
    private ReactivePulsarClient reactiveClient;

    private final String topic = UUID.randomUUID().toString();

    @BeforeEach
    void createClients() throws Exception {
        coreClient = PulsarClient.builder()
                .serviceUrl(pulsarBrokerUrl())
                .build();
        reactiveClient = ReactivePulsarClient.from(coreClient);
    }

    @AfterEach
    void cleanUp() throws Exception {
        reactiveClient.close();
    }

    @Test
    void returnsPartitionsViaGetPartitionsForTopic() throws Exception {
        triggerTopicCreation();

        reactiveClient.getPartitionsForTopic(topic)
                .as(StepVerifier::create)
                .expectNext(singletonList(topic))
                .verifyComplete();
    }

    private void triggerTopicCreation() throws PulsarClientException {
        try (Producer<byte[]> producer = coreClient.newProducer().topic(topic).create()) {
            producer.send(intToBytes(0));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private byte[] intToBytes(int n) {
        String str = Integer.toString(n);
        return str.getBytes(UTF_8);
    }
}
