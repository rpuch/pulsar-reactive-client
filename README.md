[![Maven Central](https://img.shields.io/maven-central/v/com.rpuch.pulsar-reactive-client/pulsar-client-reactor.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.rpuch.pulsar-reactive-client%22%20AND%20a:%22pulsar-client-reactor%22)
[![Build Status](https://travis-ci.org/rpuch/pulsar-reactive-client.svg?branch=master)](https://travis-ci.org/rpuch/pulsar-reactive-client)

# Pulsar Reactive Client #

This is a wrapper around asyncronous facilities provided by the official
[Apache Pulsar Java Client](https://github.com/apache/pulsar/tree/master/pulsar-client) using
[Reactor Core](https://github.com/reactor/reactor-core) interfaces.

## How to use ##

### Maven ###

```xml
<dependency>
  <groupId>com.rpuch.pulsar-reactive-client</groupId>
  <artifactId>pulsar-client-reactor</artifactId>
  <version>1.0.0</version>
</dependency>
```

### Gradle ###

```
implementation 'com.rpuch.pulsar-reactive-client:pulsar-client-reactor:1.0.0'
```

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

### Receive an infinite stream of messages starting at the very beginning of a topic ###

```java
Flux<Message<byte[]>> messages = client.newReader()
        .topic("my-topic")
        .startMessageId(MessageId.earliest)
        .messages();
messages.map(msg -> new String(msg.getData())).subscribe(System.out::println);
```

## Missing features, coming soon ##

 * Support for reactive `Consumer` interface variation (with ability to acknowledge messages on accept)
 * Support for transactions
 * Addition of support for `RxJava` and alternatives Reactive Streams implementations (like Mutiny)
 is under consideration
