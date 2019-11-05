package com.oracle.athena.webserver.connectionstate;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum BufferStateEnum {
    INVALID_STATE(0),
    READ_FROM_CHAN(1),
    READ_WAIT_FOR_DATA(2),
    READ_DONE(3),
    READ_ERROR(4),
    PARSE_HTTP_DONE(10),
    SEND_GET_DATA_RESPONSE(20),
    SEND_FINAL_RESPONSE(30);

    private int value;

    BufferStateEnum(final int value) {
        this.value = value;
    }

    public int toInt() {
        return this.value;
    }

    private static Map<Integer, BufferStateEnum> reverseLookup =
            Arrays.stream(BufferStateEnum.values()).collect(Collectors.toMap(BufferStateEnum::toInt, Function.identity()));

    public static BufferStateEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_STATE);
    }

}
