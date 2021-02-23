package com.rpuch.pulsar.reactor.api;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Mono;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactiveProducer<T> {

    /**
     * @return the topic which producer is publishing to
     */
    String getTopic();

    /**
     * @return the producer name which could have been assigned by the system or specified by the client
     */
    String getProducerName();

    Mono<MessageId> send(T message);

    /**
     * Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
     */
    Mono<Void> flush();

    /**
     * Create a new message builder.
     *
     * <p>This message builder allows to specify additional properties on the message. For example:
     * <pre>{@code
     * producer.newMessage()
     *       .key(messageKey)
     *       .value(myValue)
     *       .property("user-defined-property", "value")
     *       .send();
     * }</pre>
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     */
    ReactiveTypedMessageBuilder<T> newMessage();

    /**
     * Create a new message builder with schema, not required same parameterized type with the producer.
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     * @see #newMessage()
     */
    <V> ReactiveTypedMessageBuilder<V> newMessage(Schema<V> schema);

    /**
     * Get the last sequence id that was published by this producer.
     *
     * <p>This represent either the automatically assigned
     * or custom sequence id (set on the {@link ReactiveTypedMessageBuilder})
     * that was published and acknowledged by the broker.
     *
     * <p>After recreating a producer with the same producer name, this will return the last message that was
     * published in the previous producer session, or -1 if there no message was ever published.
     *
     * @return the last sequence id published by this producer
     */
    long getLastSequenceId();

    /**
     * Get statistics for the producer.
     * <ul>
     * <li>numMsgsSent : Number of messages sent in the current interval
     * <li>numBytesSent : Number of bytes sent in the current interval
     * <li>numSendFailed : Number of messages failed to send in the current interval
     * <li>numAcksReceived : Number of acks received in the current interval
     * <li>totalMsgsSent : Total number of messages sent
     * <li>totalBytesSent : Total number of bytes sent
     * <li>totalSendFailed : Total number of messages failed to send
     * <li>totalAcksReceived: Total number of acks received
     * </ul>
     *
     * @return statistic for the producer or null if ProducerStatsRecorderImpl is disabled.
     */
    ProducerStats getStats();

    /**
     * @return Whether the producer is currently connected to the broker
     */
    boolean isConnected();

    /**
     * @return The last disconnected timestamp of the producer
     */
    long getLastDisconnectedTimestamp();
}
