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
package com.rpuch.pulsar.reactor.it;

import com.rpuch.pulsar.reactor.api.ReactivePulsarClient;
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
