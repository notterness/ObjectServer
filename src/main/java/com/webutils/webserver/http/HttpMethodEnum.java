package com.webutils.webserver.http;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum HttpMethodEnum {

    INVALID_METHOD(0, "INVALID METHOD"),
    PUT_METHOD(10, "PUT"),
    POST_METHOD(20, "POST"),
    POST_TENANCY_METHOD(21, "POST_TENANCY"),
    POST_USER_METHOD(22, "POST_USER"),
    GET_METHOD(30, "GET"),
    DELETE_METHOD(40, "DELETE"),
    LIST_METHOD(50, "LIST"),
    LIST_SERVERS_METHOD(60, "LIST_SERVERS"),
    LIST_CHUNKS_METHOD(61, "LIST_CHUNKS"),

    DELETE_BUCKET(70, "DELETE_BUCKET"),

    HEALTH_CHECK(90, "HEALTH_CHECK");

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
