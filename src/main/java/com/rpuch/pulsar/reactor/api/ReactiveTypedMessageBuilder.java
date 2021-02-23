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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Roman Puchkovskiy
 */
public interface ReactiveTypedMessageBuilder<T> {

    Mono<MessageId> send();

    /**
     * Sets the key of the message for routing policy.
     *
     * @param key the partitioning key for the message
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> key(String key);

    /**
     * Sets the bytes of the key of the message for routing policy.
     * Internally the bytes will be base64 encoded.
     *
     * @param key routing key for message, in byte array form
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> keyBytes(byte[] key);

    /**
     * Sets the ordering key of the message for message dispatch in {@link SubscriptionType#Key_Shared} mode.
     * Partition key Will be used if ordering key not specified.
     *
     * @param orderingKey the ordering key for the message
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> orderingKey(byte[] orderingKey);

    /**
     * Set a domain object on the message.
     *
     * @param value
     *            the domain object
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> value(T value);

    /**
     * Sets a new property on a message.
     *
     * @param name
     *            the name of the property
     * @param value
     *            the associated value
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> property(String name, String value);

    /**
     * Add all the properties in the provided map.
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> properties(Map<String, String> properties);

    /**
     * Set the event time for a given message.
     *
     * <p>Applications can retrieve the event time by calling {@link Message#getEventTime()}.
     *
     * <p>Note: currently pulsar doesn't support event-time based index. so the subscribers
     * can't seek the messages by event time.
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> eventTime(long timestamp);

    /**
     * Specify a custom sequence id for the message being published.
     *
     * <p>The sequence id can be used for deduplication purposes and it needs to follow these rules:
     * <ol>
     * <li><code>sequenceId >= 0</code>
     * <li>Sequence id for a message needs to be greater than sequence id for earlier messages:
     * <code>sequenceId(N+1) > sequenceId(N)</code>
     * <li>It's not necessary for sequence ids to be consecutive. There can be holes between messages. Eg. the
     * <code>sequenceId</code> could represent an offset or a cumulative size.
     * </ol>
     *
     * @param sequenceId
     *            the sequence id to assign to the current message
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> sequenceId(long sequenceId);

    /**
     * Override the geo-replication clusters for this message.
     *
     * @param clusters the list of clusters.
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> replicationClusters(List<String> clusters);

    /**
     * Disable geo-replication for this message.
     *
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> disableReplication();

    /**
     * Deliver the message only at or after the specified absolute timestamp.
     *
     * <p>The timestamp is milliseconds and based on UTC (eg: {@link System#currentTimeMillis()}.
     *
     * <p><b>Note</b>: messages are only delivered with delay when a consumer is consuming
     * through a {@link SubscriptionType#Shared} subscription. With other subscription
     * types, the messages will still be delivered immediately.
     *
     * @param timestamp
     *            absolute timestamp indicating when the message should be delivered to consumers
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> deliverAt(long timestamp);

    /**
     * Request to deliver the message only after the specified relative delay.
     *
     * <p><b>Note</b>: messages are only delivered with delay when a consumer is consuming
     * through a {@link SubscriptionType#Shared} subscription. With other subscription
     * types, the messages will still be delivered immediately.
     *
     * @param delay
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit);

    /**
     * Configure the {@link ReactiveTypedMessageBuilder} from a config map, as an alternative compared
     * to call the individual builder methods.
     *
     * <p>The "value" of the message itself cannot be set on the config map.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Map<String, Object> conf = new HashMap<>();
     * conf.put("key", "my-key");
     * conf.put("eventTime", System.currentTimeMillis());
     *
     * producer.newMessage()
     *             .value("my-message")
     *             .loadConf(conf)
     *             .send();
     * }</pre>
     *
     * <p>The available options are:
     * <table border="1">
     *  <tr>
     *    <th>Constant</th>
     *    <th>Name</th>
     *    <th>Type</th>
     *    <th>Doc</th>
     *  </tr>
     *  <tr>
     *    <td>{@link TypedMessageBuilder#CONF_KEY}</td>
     *    <td>{@code key}</td>
     *    <td>{@code String}</td>
     *    <td>{@link #key(String)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link TypedMessageBuilder#CONF_PROPERTIES}</td>
     *    <td>{@code properties}</td>
     *    <td>{@code Map<String,String>}</td>
     *    <td>{@link #properties(Map)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link TypedMessageBuilder#CONF_EVENT_TIME}</td>
     *    <td>{@code eventTime}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #eventTime(long)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link TypedMessageBuilder#CONF_SEQUENCE_ID}</td>
     *    <td>{@code sequenceId}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #sequenceId(long)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link TypedMessageBuilder#CONF_REPLICATION_CLUSTERS}</td>
     *    <td>{@code replicationClusters}</td>
     *    <td>{@code List<String>}</td>
     *    <td>{@link #replicationClusters(List)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link TypedMessageBuilder#CONF_DISABLE_REPLICATION}</td>
     *    <td>{@code disableReplication}</td>
     *    <td>{@code boolean}</td>
     *    <td>{@link #disableReplication()}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link TypedMessageBuilder#CONF_DELIVERY_AFTER_SECONDS}</td>
     *    <td>{@code deliverAfterSeconds}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #deliverAfter(long, TimeUnit)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link TypedMessageBuilder#CONF_DELIVERY_AT}</td>
     *    <td>{@code deliverAt}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #deliverAt(long)}</td>
     *  </tr>
     * </table>
     *
     * @param config a map with the configuration options for the message
     * @return the message builder instance
     */
    ReactiveTypedMessageBuilder<T> loadConf(Map<String, Object> config);
}
