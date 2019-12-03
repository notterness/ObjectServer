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
    CHECK_SLOW_CHANNEL(2),
    ALLOC_HTTP_BUFFER(10),
    READ_HTTP_BUFFER(11),
    READ_NEXT_HTTP_BUFFER(12),
    PARSE_HTTP_BUFFER(13),
    PARSE_REST_CMD(14),
    VALIDATE_OBJECT_TRANSACTION(15),
    SETUP_NEXT_PIPELINE(17),
    SETUP_CONTENT_READ(20),
    READ_FROM_CHAN(21),
    READ_WAIT_FOR_DATA(22),
    READ_DONE(23),
    READ_NEXT_BUFFER(24),
    READ_DONE_ERROR(25),
    PROCESS_READ_ERROR(26),
    WRITE_DATA(30),
    WRITE_RESULT(31),
    CONN_FINISHED(32),
    WRITE_ERROR(33),
    ALLOC_CONTENT_DATA_BUFFER(40),
    READ_CONTENT_DATA(41),
    CLIENT_READ_CB(42),
    MD5_CALCULATE(43),
    MD5_BUFFER_DONE(44),
    MD5_CALCULATE_COMPLETE(45),
    SEND_OUT_OF_RESOURCE_RESPONSE(50),
    SEND_TIMEOUT_RESPONSE(51),
    SEND_FINAL_RESPONSE(53),
    PROCESS_FINAL_RESPONSE_SEND(54),
    SSL_HANDSHAKE(60),
    SSL_ALLOC_BUFFERS(61),
    UNWRAP_HTTP_BUFFER(62),
    UNWRAP_DATA_BUFFER(63),
    SSL_CONN_CLOSE(64),
    FREE_HTTP_BUFFERS(65),
    RUN_AUTHENTICATION_PIPELINE(120),   // Used by HttpParsePipelineMgr and HttpsParsePipelineMgr
    CHECK_EMBARGO(122),                 // This is used by the AuthenticatePipelineMgr
    AUTHENTICATE_REQUEST(123),
    AUTHENTICATE_FINISHED(124);

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
