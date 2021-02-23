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

import com.rpuch.pulsar.reactor.utils.Poller;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * @author Roman Puchkovskiy
 */
public abstract class TestWithPulsar {
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:2.7.0"));

    static {
        pulsar.start();
        waitTillReady(pulsar);
    }

    private static void waitTillReady(PulsarContainer pulsar) {
        Poller poller = new Poller(Duration.ofSeconds(10));
        try {
            poller.pollTill("Did not see public tenant become available",
                    () -> tenantPublicExists(pulsar));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static boolean tenantPublicExists(PulsarContainer pulsar) {
        try (PulsarAdmin admin = pulsarAdmin(pulsar)) {
            return admin.tenants().getTenants().contains("public");
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    private static PulsarAdmin pulsarAdmin(PulsarContainer pulsar) {
        try {
            return PulsarAdmin.builder()
                    .serviceHttpUrl(pulsar.getHttpServiceUrl())
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    protected static String pulsarBrokerUrl() {
        return pulsar.getPulsarBrokerUrl();
    }

    protected static String adminServiceUrl() {
        return pulsar.getHttpServiceUrl();
    }
}
