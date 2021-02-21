package com.rpuch.pulsar.reactive.impl;

import com.rpuch.pulsar.reactive.api.ReactiveReader;
import com.rpuch.pulsar.reactive.reactor.ChainStream;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Reader;
import reactor.core.publisher.Flux;

/**
 * @author Roman Puchkovskiy
 */
public class ReactiveReaderImpl<T> implements ReactiveReader<T> {
    private final Reader<T> reader;

    public ReactiveReaderImpl(Reader<T> reader) {
        this.reader = reader;
    }

    @Override
    public Flux<Message<T>> receive() {
        return ChainStream.infiniteChain(reader::readNextAsync);
    }
}
