package com.oracle.athena.webserver.operations;

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
    METER_BUFFERS(2),
    READ_BUFFER(3),
    PARSE_HTTP_BUFFER(10),
    DETERMINE_REQUEST_TYPE(11),
    SETUP_V2_PUT(20),

    RUN_AUTHENTICATION_PIPELINE(120),   // Used by HttpParsePipelineMgr and HttpsParsePipelineMgr
    CHECK_EMBARGO(122),                 // This is used by the AuthenticatePipelineMgr
    AUTHENTICATE_REQUEST(123),
    AUTHENTICATE_FINISHED(124),
    ENCRYPT_BUFFER(130),
    SETUP_CHUNK_WRITE(150),
    WRITE_TO_STORAGE_SERVER(200),
    SEND_FINAL_STATUS(300),
    REQUEST_FINISHED(301);

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
