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

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * @author Roman Puchkovskiy
 */
public interface ReactiveConsumerBuilder<T> extends Cloneable {

    <U> Mono<U> forOne(Function<? super ReactiveConsumer<T>, ? extends Mono<U>> transformation);

    <U> Flux<U> forMany(Function<? super ReactiveConsumer<T>, ? extends Flux<U>> transformation);

    ReactiveConsumerBuilder<T> clone();

    ReactiveConsumerBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Specify the topics this consumer will subscribe on.
     *
     * @param topicNames a set of topic that the consumer will subscribe on
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> topic(String... topicNames);

    /**
     * Specify a list of topics that this consumer will subscribe on.
     *
     * @param topicNames a list of topic that the consumer will subscribe on
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> topics(List<String> topicNames);

    /**
     * Specify a pattern for topics that this consumer will subscribe on.
     *
     * <p>The pattern will be applied to subscribe to all the topics, within a single namespace, that will match the
     * pattern.
     *
     * <p>The consumer will automatically subscribe to topics created after itself.
     *
     * @param topicsPattern
     *            a regular expression to select a list of topics to subscribe to
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> topicsPattern(Pattern topicsPattern);

    /**
     * Specify a pattern for topics that this consumer will subscribe on.
     *
     * <p>It accepts regular expression and will be compiled into a pattern internally. Eg.
     * "persistent://public/default/pattern-topic-.*"
     *
     * <p>The pattern will be applied to subscribe to all the topics, within a single namespace, that will match the
     * pattern.
     *
     * <p>The consumer will automatically subscribe to topics created after itself.
     *
     * @param topicsPattern
     *            given regular expression for topics pattern
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> topicsPattern(String topicsPattern);

    /**
     * Specify the subscription name for this consumer.
     *
     * <p>This argument is required when constructing the consumer.
     *
     * @param subscriptionName the name of the subscription that this consumer should attach to
     *
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> subscriptionName(String subscriptionName);

    /**
     * Set the timeout for unacked messages, truncated to the nearest millisecond. The timeout needs to be greater than
     * 1 second.
     *
     * <p>By default, the acknowledge timeout is disabled and that means that messages delivered to a
     * consumer will not be re-delivered unless the consumer crashes.
     *
     * <p>When enabling ack timeout, if a message is not acknowledged within the specified timeout
     * it will be re-delivered to the consumer (possibly to a different consumer in case of
     * a shared subscription).
     *
     * @param ackTimeout
     *            for unacked messages.
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> ackTimeout(long ackTimeout, TimeUnit timeUnit);

    /**
     * Define the granularity of the ack-timeout redelivery.
     *
     * <p>By default, the tick time is set to 1 second. Using an higher tick time will
     * reduce the memory overhead to track messages when the ack-timeout is set to
     * bigger values (eg: 1hour).
     *
     * @param tickTime
     *            the min precision for the ack timeout messages tracker
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> ackTimeoutTickTime(long tickTime, TimeUnit timeUnit);

    /**
     * Set the delay to wait before re-delivering messages that have failed to be process.
     *
     * <p>When application uses {@link ReactiveConsumer#negativeAcknowledge(Message)}, the failed message
     * will be redelivered after a fixed timeout. The default is 1 min.
     *
     * @param redeliveryDelay
     *            redelivery delay for failed messages
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     * @see ReactiveConsumer#negativeAcknowledge(Message)
     */
    ReactiveConsumerBuilder<T> negativeAckRedeliveryDelay(long redeliveryDelay, TimeUnit timeUnit);

    /**
     * Select the subscription type to be used when subscribing to the topic.
     *
     * <p>Options are:
     * <ul>
     *  <li>{@link SubscriptionType#Exclusive} (Default)</li>
     *  <li>{@link SubscriptionType#Failover}</li>
     *  <li>{@link SubscriptionType#Shared}</li>
     * </ul>
     *
     * @param subscriptionType
     *            the subscription type value
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> subscriptionType(SubscriptionType subscriptionType);

    /**
     * Select the subscription mode to be used when subscribing to the topic.
     *
     * <p>Options are:
     * <ul>
     *  <li>{@link SubscriptionMode#Durable} (Default)</li>
     *  <li>{@link SubscriptionMode#NonDurable}</li>
     * </ul>
     *
     * @param subscriptionMode
     *            the subscription mode value
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> subscriptionMode(SubscriptionMode subscriptionMode);

    /**
     * Sets a {@link MessageListener} for the consumer
     *
     * <p>When a {@link MessageListener} is set, application will receive messages through it. Calls to
     * {@link Consumer#receive()} will not be allowed.
     *
     * @param messageListener
     *            the listener object
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> messageListener(MessageListener<T> messageListener);

    /**
     * Sets a {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt the message payloads.
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets a {@link MessageCrypto}.
     *
     * <p>Contains methods to encrypt/decrypt message for End to End Encryption.
     *
     * @param messageCrypto
     *            MessageCrypto object
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> messageCrypto(MessageCrypto messageCrypto);

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified.
     *
     * @param action
     *            the action the consumer will take in case of decryption failures
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);

    /**
     * Sets the size of the consumer receive queue.
     *
     * <p>The consumer receive queue controls how many messages can be accumulated by the {@link ReactiveConsumer} before the
     * application calls {@link ReactiveConsumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     *
     * <p><b>Setting the consumer queue size as zero</b>
     * <ul>
     * <li>Decreases the throughput of the consumer, by disabling pre-fetching of messages. This approach improves the
     * message distribution on shared subscription, by pushing messages only to the consumers that are ready to process
     * them. Neither {@link ReactiveConsumer#receive(int, TimeUnit)} nor Partitioned Topics can be used if the consumer queue
     * size is zero. {@link ReactiveConsumer#receive()} function call should not be interrupted when the consumer queue size is
     * zero.</li>
     * <li>Doesn't support Batch-Message: if consumer receives any batch-message then it closes consumer connection with
     * broker and {@link ReactiveConsumer#receive()} call will remain blocked while {@link ReactiveConsumer#receiveAsync()} receives
     * exception in callback. <b> consumer will not be able receive any further message unless batch-message in pipeline
     * is removed</b></li>
     * </ul>
     * Default value is {@code 1000} messages and should be good for most use cases.
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> receiverQueueSize(int receiverQueueSize);

    /**
     * Group the consumer acknowledgments for the specified time.
     *
     * <p>By default, the consumer will use a 100 ms grouping time to send out the acknowledgments to the broker.
     *
     * <p>Setting a group time of 0, will send out the acknowledgments immediately. A longer ack group time
     * will be more efficient at the expense of a slight increase in message re-deliveries after a failure.
     *
     * @param delay
     *            the max amount of time an acknowledgemnt can be delayed
     * @param unit
     *            the time unit for the delay
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit);

    /**
     *
     * @param replicateSubscriptionState
     */
    ReactiveConsumerBuilder<T> replicateSubscriptionState(boolean replicateSubscriptionState);

    /**
     * Set the max total receiver queue size across partitons.
     *
     * <p>This setting will be used to reduce the receiver queue size for individual partitions
     * {@link #receiverQueueSize(int)} if the total exceeds this value (default: 50000).
     * The purpose of this setting is to have an upper-limit on the number
     * of messages that a consumer can be pushed at once from a broker, across all
     * the partitions.
     *
     * @param maxTotalReceiverQueueSizeAcrossPartitions
     *            max pending messages across all the partitions
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions);

    /**
     * Set the consumer name.
     *
     * <p>Consumer name is informative and it can be used to indentify a particular consumer
     * instance from the topic stats.
     *
     * @param consumerName
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> consumerName(String consumerName);

    /**
     * Sets a {@link ConsumerEventListener} for the consumer.
     *
     * <p>The consumer group listener is used for receiving consumer state change in a consumer group for failover
     * subscription. Application can then react to the consumer state changes.
     *
     * @param consumerEventListener
     *            the consumer group listener object
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> consumerEventListener(ConsumerEventListener consumerEventListener);

    /**
     * If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog
     * of the topic. This means that, if the topic has been compacted, the consumer will only see the latest value for
     * each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
     * point, the messages will be sent as normal.
     *
     * <p>readCompacted can only be enabled subscriptions to persistent topics, which have a single active consumer
     * (i.e. failure or exclusive subscriptions). Attempting to enable it on subscriptions to a non-persistent topics
     * or on a shared subscription, will lead to the subscription call throwing a PulsarClientException.
     *
     * @param readCompacted
     *            whether to read from the compacted topic
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> readCompacted(boolean readCompacted);

    /**
     * Set topics auto discovery period when using a pattern for topics consumer.
     * The period is in minute, and default and minimum value is 1 minute.
     *
     * @param periodInMinutes
     *            number of minutes between checks for
     *            new topics matching pattern set with {@link #topicsPattern(String)}
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> patternAutoDiscoveryPeriod(int periodInMinutes);


    /**
     * Set topics auto discovery period when using a pattern for topics consumer.
     *
     * @param interval
     *            the amount of delay between checks for
     *            new topics matching pattern set with {@link #topicsPattern(String)}
     * @param unit
     *            the unit of the topics auto discovery period
     *
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> patternAutoDiscoveryPeriod(int interval, TimeUnit unit);


    /**
     * <b>Shared subscription</b>
     * Sets priority level for the shared subscription consumers to which broker gives more priority while dispatching
     * messages. Here, broker follows descending priorities. (eg: 0=max-priority, 1, 2,..)
     *
     * <p>In Shared subscription mode, broker will first dispatch messages to max priority-level
     * consumers if they have permits, else broker will consider next priority level consumers.
     *
     * <p>If subscription has consumer-A with priorityLevel 0 and Consumer-B with priorityLevel 1
     * then broker will dispatch messages to only consumer-A until it runs out permit and then broker
     * starts dispatching messages to Consumer-B.
     *
     * <p><pre>
     * Consumer PriorityLevel Permits
     * C1       0             2
     * C2       0             1
     * C3       0             1
     * C4       1             2
     * C5       1             1
     * Order in which broker dispatches messages to consumers: C1, C2, C3, C1, C4, C5, C4
     * </pre>
     *
     * <p><b>Failover subscription</b>
     * Broker selects active consumer for a failover-subscription based on consumer's priority-level and
     * lexicographical sorting of a consumer name.
     * eg:
     * <pre>
     * 1. Active consumer = C1 : Same priority-level and lexicographical sorting
     * Consumer PriorityLevel Name
     * C1       0             aaa
     * C2       0             bbb
     *
     * 2. Active consumer = C2 : Consumer with highest priority
     * Consumer PriorityLevel Name
     * C1       1             aaa
     * C2       0             bbb
     *
     * Partitioned-topics:
     * Broker evenly assigns partitioned topics to highest priority consumers.
     * </pre>
     *
     * @param priorityLevel the priority of this consumer
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> priorityLevel(int priorityLevel);

    /**
     * Set a name/value property with this consumer.
     *
     * <p>Properties are application defined metadata that can be attached to the consumer.
     * When getting the topic stats, this metadata will be associated to the consumer stats for easier identification.
     *
     * @param key
     *            the property key
     * @param value
     *            the property value
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> property(String key, String value);

    /**
     * Add all the properties in the provided map to the consumer.
     *
     * <p>Properties are application defined metadata that can be attached to the consumer.
     * When getting the topic stats, this metadata will be associated to the consumer stats for easier identification.
     *
     * @param properties the map with properties
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> properties(Map<String, String> properties);

    /**
     * Set the {@link SubscriptionInitialPosition} for the consumer.
     *
     * @param subscriptionInitialPosition
     *            the position where to initialize a newly created subscription
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition);

    /**
     * Determines to which topics this consumer should be subscribed to - Persistent, Non-Persistent, or both. Only used
     * with pattern subscriptions.
     *
     * @param regexSubscriptionMode
     *            Pattern subscription mode
     */
    ReactiveConsumerBuilder<T> subscriptionTopicsMode(RegexSubscriptionMode regexSubscriptionMode);

    /**
     * Intercept {@link ReactiveConsumer}.
     *
     * @param interceptors the list of interceptors to intercept the consumer created by this builder.
     */
    ReactiveConsumerBuilder<T> intercept(ConsumerInterceptor<T>...interceptors);

    /**
     * Set dead letter policy for consumer.
     *
     * <p>By default some message will redelivery so many times possible, even to the extent that it can be never stop.
     * By using dead letter mechanism messages will has the max redelivery count, when message exceeding the maximum
     * number of redeliveries, message will send to the Dead Letter Topic and acknowledged automatic.
     *
     * <p>You can enable the dead letter mechanism by setting dead letter policy.
     * example:
     * <pre>
     * client.newConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())
     *          .subscribe();
     * </pre>
     * Default dead letter topic name is {TopicName}-{Subscription}-DLQ.
     * To setting a custom dead letter topic name
     * <pre>
     * client.newConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy
     *              .builder()
     *              .maxRedeliverCount(10)
     *              .deadLetterTopic("your-topic-name")
     *              .build())
     *          .subscribe();
     * </pre>
     * When a dead letter policy is specified, and no ackTimeoutMillis is specified,
     * then the ack timeout will be set to 30000 millisecond.
     */
    ReactiveConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy);

    /**
     * If enabled, the consumer will auto subscribe for partitions increasement.
     * This is only for partitioned consumer.
     *
     * @param autoUpdate
     *            whether to auto update partition increasement
     */
    ReactiveConsumerBuilder<T> autoUpdatePartitions(boolean autoUpdate);

    /**
     * Set the interval of updating partitions <i>(default: 1 minute)</i>. This only works if autoUpdatePartitions is
     * enabled.
     *
     * @param interval
     *            the interval of updating partitions
     * @param unit
     *            the time unit of the interval.
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit);

    /**
     * Set KeyShared subscription policy for consumer.
     *
     * <p>By default, KeyShared subscription use auto split hash range to maintain consumers. If you want to
     * set a different KeyShared policy, you can set by following example:
     *
     * <pre>
     * client.newConsumer()
     *          .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(0, 10)))
     *          .subscribe();
     * </pre>
     * Details about sticky hash range policy, please see {@link KeySharedPolicy.KeySharedPolicySticky}.
     *
     * <p>Or
     * <pre>
     * client.newConsumer()
     *          .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
     *          .subscribe();
     * </pre>
     * Details about auto split hash range policy, please see {@link KeySharedPolicy.KeySharedPolicyAutoSplit}.
     *
     * @param keySharedPolicy The {@link KeySharedPolicy} want to specify
     */
    ReactiveConsumerBuilder<T> keySharedPolicy(KeySharedPolicy keySharedPolicy);

    /**
     * Set the consumer to include the given position of any reset operation like {@link ReactiveConsumer#seek(long) or
     * {@link ReactiveConsumer#seek(MessageId)}}.
     *
     * @return the consumer builder instance
     */
    ReactiveConsumerBuilder<T> startMessageIdInclusive();

    /**
     * Set batch receive policy {@link BatchReceivePolicy} for consumer.
     * By default, consumer will use {@link BatchReceivePolicy#DEFAULT_POLICY} as batch receive policy.
     *
     * <p>Example:
     * <pre>
     * client.newConsumer().batchReceivePolicy(BatchReceivePolicy.builder()
     *              .maxNumMessages(100)
     *              .maxNumBytes(5 * 1024 * 1024)
     *              .timeout(100, TimeUnit.MILLISECONDS)
     *              .build()).subscribe();
     * </pre>
     */
    ReactiveConsumerBuilder<T> batchReceivePolicy(BatchReceivePolicy batchReceivePolicy);

    /**
     * If enabled, the consumer will auto retry message.
     * default unabled.
     *
     * @param retryEnable
     *            whether to auto retry message
     */
    ReactiveConsumerBuilder<T> enableRetry(boolean retryEnable);

    /**
     * Enable or disable the batch index acknowledgment. To enable this feature must ensure batch index acknowledgment
     * feature is enabled at the broker side.
     */
    ReactiveConsumerBuilder<T> enableBatchIndexAcknowledgment(boolean batchIndexAcknowledgmentEnabled);

    /**
     * Consumer buffers chunk messages into memory until it receives all the chunks of the original message. While
     * consuming chunk-messages, chunks from same message might not be contiguous in the stream and they might be mixed
     * with other messages' chunks. so, consumer has to maintain multiple buffers to manage chunks coming from different
     * messages. This mainly happens when multiple publishers are publishing messages on the topic concurrently or
     * publisher failed to publish all chunks of the messages.
     *
     * <pre>
     * eg: M1-C1, M2-C1, M1-C2, M2-C2
     * Here, Messages M1-C1 and M1-C2 belong to original message M1, M2-C1 and M2-C2 messages belong to M2 message.
     * </pre>
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it can be
     * guarded by providing this @maxPendingChunkedMessage threshold. Once, consumer reaches this threshold, it drops
     * the outstanding unchunked-messages by silently acking or asking broker to redeliver later by marking it unacked.
     * This behavior can be controlled by configuration: @autoAckOldestChunkedMessageOnQueueFull
     *
     * @default 100
     *
     * @param maxPendingChunkedMessage
     * @return
     */
    ReactiveConsumerBuilder<T> maxPendingChunkedMessage(int maxPendingChunkedMessage);

    /**
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it can be
     * guarded by providing this @maxPendingChunkedMessage threshold. Once, consumer reaches this threshold, it drops
     * the outstanding unchunked-messages by silently acking if autoAckOldestChunkedMessageOnQueueFull is true else it
     * marks them for redelivery.
     *
     * @default false
     *
     * @param autoAckOldestChunkedMessageOnQueueFull
     * @return
     */
    ReactiveConsumerBuilder<T> autoAckOldestChunkedMessageOnQueueFull(boolean autoAckOldestChunkedMessageOnQueueFull);

    /**
     * If producer fails to publish all the chunks of a message then consumer can expire incomplete chunks if consumer
     * won't be able to receive all chunks in expire times (default 1 hour).
     *
     * @param duration
     * @param unit
     * @return
     */
    ReactiveConsumerBuilder<T> expireTimeOfIncompleteChunkedMessage(long duration, TimeUnit unit);
}
