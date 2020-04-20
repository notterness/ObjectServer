package com.webutils.webserver.http;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum HttpMethodEnum {

    INVALID_METHOD(0),
    PUT_METHOD(1),
    PUT_STORAGE_SERVER(2),
    POST_METHOD(3);

    private final int value;

    HttpMethodEnum(final int value) {
        this.value = value;
    }

    public int toInt() {
        return this.value;
    }

    private final static Map<Integer, HttpMethodEnum> reverseLookup =
            Arrays.stream(HttpMethodEnum.values()).collect(Collectors.toMap(HttpMethodEnum::toInt, Function.identity()));

    public final static HttpMethodEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_METHOD);
    }
}
