[![Build Status](https://travis-ci.org/rpuch/pulsar-reactive-client.svg?branch=master)](https://travis-ci.org/rpuch/pulsar-reactive-client)

# Pulsar Reactive Client #

This is a wrapper around asyncronous facilities provided by the official
[Apache Pulsar Java Client](https://github.com/apache/pulsar/tree/master/pulsar-client) using
[Reactor Core](https://github.com/reactor/reactor-core) interfaces.

## How to use ###

### Create a reactive client ###

```java
PulsarClient coreClient = PulsarClient.builder().serviceUrl(pulsarBrokerUrl).build();
ReactivePulsarClient client = ReactivePulsarClient.from(coreClient);
```

### Send a message ###

```java
MessageId messageId = client.newProducer()
        .topic("my-topic")
        .forOne(producer -> producer.send("Hello!".bytes()))
        .block();
```

### Receive an infinite stream of messages ###

```java
Flux<Message<byte[]>> messages = client.newReader()
        .topic("my-topic")
        .messages();
messages.map(msg -> new String(msg.getData())).subscribe(System.out::println);
```

## Missing features, coming soon ##

 * Support for reactive `Consumer` interface variation (with ability to acknowledge messages on accept)
 * Support for transactions
 * Addition of support for `RxJava` and alternatives Reactive Streams implementations (like Mutiny)
 is under consideration
