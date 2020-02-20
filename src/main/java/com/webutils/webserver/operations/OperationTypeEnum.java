package com.webutils.webserver.operations;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


/*
 ** This is used to track the states that a connection can go through.
 */
public enum OperationTypeEnum {

    INVALID_STATE(0),
    INITIAL_SETUP(1),
    METER_READ_BUFFERS(2),
    READ_BUFFER(3),
    PARSE_HTTP_BUFFER(10),
    DETERMINE_REQUEST_TYPE(11),
    SETUP_V2_PUT(20),

    HANDLE_CLIENT_ERROR(50),

    RUN_AUTHENTICATION_PIPELINE(120),   // Used by HttpParsePipelineMgr and HttpsParsePipelineMgr
    CHECK_EMBARGO(122),                 // This is used by the AuthenticatePipelineMgr
    AUTHENTICATE_REQUEST(123),
    AUTHENTICATE_FINISHED(124),
    COMPUTE_MD5_DIGEST(130),
    ENCRYPT_BUFFER(135),
    VON_PICKER(136),
    SETUP_CHUNK_WRITE_0(140),
    SETUP_CHUNK_WRITE_1(141),
    SETUP_CHUNK_WRITE_2(142),
    CONNECT_COMPLETE(160),
    HANDLE_STORAGE_SERVER_ERROR(180),
    BUILD_HEADER_TO_STORGE_SERVER(200),
    WRITE_HEADER_TO_STORAGE_SERVER(220),
    WRITE_TO_STORAGE_SERVER(240),
    READ_STORAGE_SERVER_RESPONSE_BUFFER(260),
    STORAGE_SERVER_RESPONSE_HANDLER(280),
    SEND_FINAL_STATUS(350),
    WRITE_TO_CLIENT(351),
    REQUEST_FINISHED(352),

    SETUP_STORAGE_SERVER_PUT(380),
    WRITE_TO_FILE(381),

    SETUP_CLIENT_CONNECTION(400),
    CLIENT_CONNECT_COMPLETE(401),
    CLIENT_WRITE_HTTP_HEADER(402),
    CLIENT_OBJECT_WRITE(403),
    CLIENT_WRITE(404),
    CLIENT_RESPONSE_HANDLER(405),
    HANDLE_INITIATOR_ERROR(410),

    DATABASE_SETUP(500),

    CHUNK_WRITE_TEST_COMPLETE(600);

    private int value;

    OperationTypeEnum(final int value) {
        this.value = value;
    }

    public int toInt() {
        return this.value;
    }

    private static Map<Integer, OperationTypeEnum> reverseLookup =
            Arrays.stream(OperationTypeEnum.values()).collect(Collectors.toMap(OperationTypeEnum::toInt, Function.identity()));

    public static OperationTypeEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_STATE);
    }
}