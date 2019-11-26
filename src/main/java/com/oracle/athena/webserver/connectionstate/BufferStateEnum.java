package com.oracle.athena.webserver.connectionstate;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum BufferStateEnum {
    INVALID_STATE(0),
    READ_HTTP_FROM_CHAN(10),
    READ_WAIT_FOR_HTTP(11),
    READ_HTTP_DONE(12),
    READ_DATA_FROM_CHAN(20),
    READ_WAIT_FOR_DATA(21),
    READ_DATA_DONE(22),
    READ_DONE(30),
    READ_ERROR(31),
    PARSE_HTTP_DONE(40),
    DIGEST_WAIT(41),
    DIGEST_DONE(42),
    SEND_GET_DATA_RESPONSE(50),
    SEND_FINAL_RESPONSE(60),
    SSL_HANDSHAKE_APP_BUFFER(70),
    SSL_HANDSHAKE_NET_BUFFER(71);

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
