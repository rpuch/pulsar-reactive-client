package com.rpuch.pulsar.reactive.it;

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
