package com.oracle.athena.webserver.connectionstate;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 ** This is used to track the states that a connection can go through.
 */
public enum ConnectionStateEnum {

    INVALID_STATE(0),
    INITIAL_SETUP(1),
    ALLOC_HTTP_BUFFER(10),
    READ_HTTP_BUFFER(11),
    READ_NEXT_HTTP_BUFFER(12),
    PARSE_HTTP_BUFFER(13),
    PARSE_REST_CMD(14),
    VALIDATE_OBJECT_TRANSACTION(15),
    SEND_XFR_DATA_RESP(16),
    READ_FROM_CHAN(20),
    READ_WAIT_FOR_DATA(21),
    READ_DONE(22),
    READ_NEXT_BUFFER(23),
    READ_DONE_ERROR(24),
    WRITE_DATA(30),
    WRITE_RESULT(31),
    CONN_FINISHED(32),
    WRTIE_ERROR(33),
    ALLOC_CLIENT_DATA_BUFFER(40),
    READ_CLIENT_DATA(41),
    CLIENT_READ_CB(42);

    private int value;

    ConnectionStateEnum(final int value) {
        this.value = value;
    }

    public int toInt() {
        return this.value;
    }

    private static Map<Integer, ConnectionStateEnum> reverseLookup =
            Arrays.stream(ConnectionStateEnum.values()).collect(Collectors.toMap(ConnectionStateEnum::toInt, Function.identity()));

    public static ConnectionStateEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_STATE);
    }
}
