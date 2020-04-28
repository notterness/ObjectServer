package com.webutils.webserver.http;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum HttpMethodEnum {

    INVALID_METHOD(0, "INVALID METHOD"),
    PUT_METHOD(1, "PUT"),
    POST_METHOD(2, "POST"),
    GET_METHOD(3, "GET");

    private final int value;
    private final String methodName;

    HttpMethodEnum(final int value, final String name) {

        this.value = value;
        this.methodName = name;
    }

    public int toInt() {
        return this.value;
    }

    public String toString() { return this.methodName; }

    private final static Map<Integer, HttpMethodEnum> reverseLookup =
            Arrays.stream(HttpMethodEnum.values()).collect(Collectors.toMap(HttpMethodEnum::toInt, Function.identity()));

    public static HttpMethodEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_METHOD);
    }
}
