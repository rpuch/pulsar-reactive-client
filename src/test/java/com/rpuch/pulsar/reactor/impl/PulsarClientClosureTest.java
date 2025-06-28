/*
 * Copyright 2025 Pulsar Reactive Client contributors
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
package com.rpuch.pulsar.reactor.impl;

import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.rpuch.pulsar.reactor.utils.Futures.failedFuture;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Roman Puchkovskiy
 */
@ExtendWith(MockitoExtension.class)
class PulsarClientClosureTest {
    @Mock
    private Supplier<CompletableFuture<Void>> closerSupplier;

    @Test
    void closeQuietlyIgnoresAlreadyClosedException() {
        when(closerSupplier.get())
                .thenReturn(failedFuture(new AlreadyClosedException("closed")));

        PulsarClientClosure.closeQuietly(closerSupplier)
                .as(StepVerifier::create)
                .verifyComplete();

        verify(closerSupplier).get();
    }
}
