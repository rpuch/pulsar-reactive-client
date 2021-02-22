package com.rpuch.pulsar.reactive.it;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * @author Roman Puchkovskiy
 */
public class AdminClient {
    private final String adminServiceUrl;

    public AdminClient(String adminServiceUrl) {
        this.adminServiceUrl = adminServiceUrl;
    }

    public void terminateTopic(String topic) {
        try (PulsarAdmin admin = pulsarAdmin()) {
            admin.topics().terminateTopic(topic);
        } catch (PulsarClientException | PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    private PulsarAdmin pulsarAdmin() throws PulsarClientException {
        return PulsarAdmin.builder()
                .serviceHttpUrl(adminServiceUrl)
                .build();
    }
}
