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
package com.rpuch.pulsar.reactor.api;

import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactiveConsumer<T> {

    /**
     * Get a topic for the consumer.
     *
     * @return topic for the consumer
     */
    String getTopic();

    /**
     * Get a subscription for the consumer.
     *
     * @return subscription for the consumer
     */
    String getSubscription();

    Mono<Void> unsubscribe();

    Flux<Message<T>> messages();

    Mono<Message<T>> receive();

    Mono<Messages<T>> batchReceive();

    Mono<Void> acknowledge(Message<?> message);

    Mono<Void> acknowledge(MessageId messageId);

    /**
     * Acknowledge the consumption of {@link Messages}.
     *
     * @param messages messages
     */
    Mono<Void> acknowledge(Messages<?> messages);

    /**
     * Acknowledge the consumption of a list of message.
     * @param messageIdList list of message IDs to acknowledge
     */
    Mono<Void> acknowledge(List<MessageId> messageIdList);

    /**
     * Acknowledge the failure to process a single message.
     *
     * <p>When a message is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with {@link ReactiveConsumerBuilder#negativeAckRedeliveryDelay(long, TimeUnit)}.
     *
     * <p>This call is not blocking.
     *
     * <p>Example of usage:
     * <pre><code>
     * while (true) {
     *     Message&lt;String&gt; msg = consumer.receive();
     *
     *     try {
     *          // Process message...
     *
     *          consumer.acknowledge(msg);
     *     } catch (Throwable t) {
     *          log.warn("Failed to process message");
     *          consumer.negativeAcknowledge(msg);
     *     }
     * }
     * </code></pre>
     *
     * @param message
     *            The {@code Message} to be acknowledged
     */
    void negativeAcknowledge(Message<?> message);

    /**
     * Acknowledge the failure to process a single message.
     *
     * <p>When a message is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with {@link ReactiveConsumerBuilder#negativeAckRedeliveryDelay(long, TimeUnit)}.
     *
     * <p>This call is not blocking.
     *
     * <p>This variation allows to pass a {@link MessageId} rather than a {@link Message}
     * object, in order to avoid keeping the payload in memory for extended amount
     * of time
     *
     * @see #negativeAcknowledge(Message)
     *
     * @param messageId
     *            The {@code MessageId} to be acknowledged
     */
    void negativeAcknowledge(MessageId messageId);

    /**
     * Acknowledge the failure to process {@link Messages}.
     *
     * <p>When messages is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with {@link ReactiveConsumerBuilder#negativeAckRedeliveryDelay(long, TimeUnit)}.
     *
     * <p>This call is not blocking.
     *
     * <p>Example of usage:
     * <pre><code>
     * while (true) {
     *     Messages&lt;String&gt; msgs = consumer.batchReceive();
     *
     *     try {
     *          // Process message...
     *
     *          consumer.acknowledge(msgs);
     *     } catch (Throwable t) {
     *          log.warn("Failed to process message");
     *          consumer.negativeAcknowledge(msgs);
     *     }
     * }
     * </code></pre>
     *
     * @param messages
     *            The {@code Message} to be acknowledged
     */
    void negativeAcknowledge(Messages<?> messages);

    Mono<Void> reconsumeLater(Message<?> message, long delayTime, TimeUnit unit);

    Mono<Void> reconsumeLater(Messages<?> messages, long delayTime, TimeUnit unit);

    Mono<Void> acknowledgeCumulative(Message<?> message);

    Mono<Void> acknowledgeCumulative(MessageId messageId);

    Mono<Void> reconsumeLaterCumulative(Message<?> message, long delayTime, TimeUnit unit);

    /**
     * Get statistics for the consumer.
     * <ul>
     * <li>numMsgsReceived : Number of messages received in the current interval
     * <li>numBytesReceived : Number of bytes received in the current interval
     * <li>numReceiveFailed : Number of messages failed to receive in the current interval
     * <li>numAcksSent : Number of acks sent in the current interval
     * <li>numAcksFailed : Number of acks failed to send in the current interval
     * <li>totalMsgsReceived : Total number of messages received
     * <li>totalBytesReceived : Total number of bytes received
     * <li>totalReceiveFailed : Total number of messages failed to receive
     * <li>totalAcksSent : Total number of acks sent
     * <li>totalAcksFailed : Total number of acks failed to sent
     * </ul>
     *
     * @return statistic for the consumer
     */
    ConsumerStats getStats();

    /**
     * Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
     *
     * <p>Please note that this does not simply mean that the consumer is caught up with the last message published by
     * producers, rather the topic needs to be explicitly "terminated".
     */
    boolean hasReachedEndOfTopic();

    /**
     * Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
     * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
     * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
     * breaks, the messages are redelivered after reconnect.
     */
    void redeliverUnacknowledgedMessages();

    /**
     * Reset the subscription associated with this consumer to a specific message id.
     *
     * <p>The message id can either be a specific message or represent the first or last messages in the topic.
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the subscription on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the subscription on the latest message in the topic
     * </ul>
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param messageId
     *            the message id where to reposition the subscription
     */
    Mono<Void> seek(MessageId messageId);

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     */
    Mono<Void> seek(long timestamp);

    /**
     * Get the last message id available available for consume.
     *
     * @return Mono with the last message id.
     */
    Mono<MessageId> getLastMessageId();

    /**
     * @return Whether the consumer is connected to the broker
     */
    boolean isConnected();

    /**
     * Get the name of consumer.
     * @return consumer name.
     */
    String getConsumerName();

    /**
     * Stop requesting new messages from the broker until {@link #resume()} is called. Note that this might cause
     * {@link #receive()} to hang until {@link #resume()} is called and new messages are pushed by the broker.
     */
    void pause();

    /**
     * Resume requesting messages from the broker.
     */
    void resume();

    /**
     * @return The last disconnected timestamp of the consumer
     */
    long getLastDisconnectedTimestamp();
}
