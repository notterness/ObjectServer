package com.oracle.athena.webserver.connectionstate;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum AuthenticateResultEnum {

    INVALID_RESULT(0),
    EMBARGO_CHECK_IN_PROGRESS(10),
    EMBARGO_PASSED(11),
    EMBARGO_FAILED(12),
    AUTHENTICATE_IN_PROGRESS(20),
    AUTHENTICATE_PASSED(21),
    AUTHENTICATE_FAILED(22);

    private int value;

    AuthenticateResultEnum(final int value) {
        this.value = value;
    }

    public int toInt() {
        return this.value;
    }

    private static Map<Integer, AuthenticateResultEnum> reverseLookup =
            Arrays.stream(AuthenticateResultEnum.values()).collect(Collectors.toMap(AuthenticateResultEnum::toInt, Function.identity()));

    public static AuthenticateResultEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_RESULT);
    }
}
