package com.rpuch.pulsar.reactive.impl;

import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class ReactiveTypedMessageBuilderImplTest {
    @InjectMocks
    private ReactiveTypedMessageBuilderImpl<String> reactiveBuilder;

    @Mock
    private TypedMessageBuilder<String> coreBuilder;

    @Test
    void setsKeyOnCoreBuilder() {
        reactiveBuilder.key("key");

        verify(coreBuilder).key("key");
    }

    @Test
    void keyReturnsSameBuilder() {
        assertThat(reactiveBuilder.key("key"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsKeyBytesOnCoreBuilder() {
        byte[] keyBytes = new byte[0];
        reactiveBuilder.keyBytes(keyBytes);

        verify(coreBuilder).keyBytes(keyBytes);
    }

    @Test
    void keyBytesReturnsSameBuilder() {
        assertThat(reactiveBuilder.keyBytes(new byte[0]), sameInstance(reactiveBuilder));
    }

    @Test
    void setsOrderingKeyOnCoreBuilder() {
        byte[] keyBytes = new byte[0];
        reactiveBuilder.orderingKey(keyBytes);

        verify(coreBuilder).orderingKey(keyBytes);
    }

    @Test
    void orderingKeyReturnsSameBuilder() {
        assertThat(reactiveBuilder.orderingKey(new byte[0]), sameInstance(reactiveBuilder));
    }

    @Test
    void setsValueOnCoreBuilder() {
        reactiveBuilder.value("v");

        verify(coreBuilder).value("v");
    }

    @Test
    void valueReturnsSameBuilder() {
        assertThat(reactiveBuilder.value("v"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsPropertyOnCoreBuilder() {
        reactiveBuilder.property("key", "value");

        verify(coreBuilder).property("key", "value");
    }

    @Test
    void propertyReturnsSameBuilder() {
        assertThat(reactiveBuilder.property("key", "value"), sameInstance(reactiveBuilder));
    }

    @Test
    void setsPropertiesOnCoreBuilder() {
        reactiveBuilder.properties(emptyMap());

        verify(coreBuilder).properties(emptyMap());
    }

    @Test
    void propertiesReturnsSameBuilder() {
        assertThat(reactiveBuilder.properties(emptyMap()), sameInstance(reactiveBuilder));
    }

    @Test
    void setsEventTimeOnCoreBuilder() {
        reactiveBuilder.eventTime(123);

        verify(coreBuilder).eventTime(123);
    }

    @Test
    void eventTimeReturnsSameBuilder() {
        assertThat(reactiveBuilder.eventTime(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsSequenceIdOnCoreBuilder() {
        reactiveBuilder.sequenceId(123);

        verify(coreBuilder).sequenceId(123);
    }

    @Test
    void sequenceIdReturnsSameBuilder() {
        assertThat(reactiveBuilder.sequenceId(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsReplicationClustersOnCoreBuilder() {
        reactiveBuilder.replicationClusters(singletonList("a"));

        verify(coreBuilder).replicationClusters(singletonList("a"));
    }

    @Test
    void replicationClustersReturnsSameBuilder() {
        assertThat(reactiveBuilder.replicationClusters(singletonList("a")), sameInstance(reactiveBuilder));
    }

    @Test
    void setsDisableReplicationOnCoreBuilder() {
        reactiveBuilder.disableReplication();

        verify(coreBuilder).disableReplication();
    }

    @Test
    void disableReplicationReturnsSameBuilder() {
        assertThat(reactiveBuilder.disableReplication(), sameInstance(reactiveBuilder));
    }

    @Test
    void setsDeliverAtOnCoreBuilder() {
        reactiveBuilder.deliverAt(123);

        verify(coreBuilder).deliverAt(123);
    }

    @Test
    void deliverAtReturnsSameBuilder() {
        assertThat(reactiveBuilder.deliverAt(123), sameInstance(reactiveBuilder));
    }

    @Test
    void setsDeliverAfterOnCoreBuilder() {
        reactiveBuilder.deliverAfter(123, TimeUnit.SECONDS);

        verify(coreBuilder).deliverAfter(123, TimeUnit.SECONDS);
    }

    @Test
    void deliverAfterReturnsSameBuilder() {
        assertThat(reactiveBuilder.deliverAfter(123, TimeUnit.SECONDS), sameInstance(reactiveBuilder));
    }

    @Test
    void invokesLoadConfOnCoreBuilder() {
        reactiveBuilder.loadConf(singletonMap("k", "v"));

        verify(coreBuilder).loadConf(singletonMap("k", "v"));
    }

    @Test
    void loadConfReturnsSameBuilder() {
        assertThat(reactiveBuilder.loadConf(singletonMap("k", "v")), sameInstance(reactiveBuilder));
    }
}