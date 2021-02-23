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
package com.rpuch.pulsar.reactor.it;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Roman Puchkovskiy
 */
public class MessageConverter {
    public byte[] intToBytes(int n) {
        String str = Integer.toString(n);
        return str.getBytes(UTF_8);
    }

    public int intFromBytes(byte[] bytes) {
        String str = new String(bytes, UTF_8);
        return Integer.parseInt(str);
    }

    public String intToString(int n) {
        return Integer.toString(n);
    }

    public Integer intFromString(String s) {
        return Integer.valueOf(s);
    }
}
