package com.oracle.athena.webserver.connectionstate;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum HttpMethodEnum {

    INVALID_METHOD(0),
    PUT_METHOD(1),
    POST_METHOD(2);

    private int value;

    HttpMethodEnum(final int value) {
        this.value = value;
    }

    public int toInt() {
        return this.value;
    }

    private static Map<Integer, HttpMethodEnum> reverseLookup =
            Arrays.stream(HttpMethodEnum.values()).collect(Collectors.toMap(HttpMethodEnum::toInt, Function.identity()));

    public static HttpMethodEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_METHOD);
    }
}
