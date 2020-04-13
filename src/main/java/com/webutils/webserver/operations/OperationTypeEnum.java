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
    SETUP_OBJECT_SERVER_POST(21),

    HANDLE_CLIENT_ERROR(50),

    RUN_AUTHENTICATION_PIPELINE(120),   // Used by HttpParsePipelineMgr and HttpsParsePipelineMgr
    CHECK_EMBARGO(122),                 // This is used by the AuthenticatePipelineMgr
    AUTHENTICATE_REQUEST(123),
    AUTHENTICATE_FINISHED(124),
    COMPUTE_MD5_DIGEST(130),
    COMPUTE_SHA256_DIGEST(131),
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
    STORAGE_SERVER_RESPONSE_BUFFER_METERING(260),
    READ_STORAGE_SERVER_RESPONSE_BUFFER(261),
    STORAGE_SERVER_RESPONSE_HANDLER(262),
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

    TEST_CHUNK_WRITE(600),
    CHUNK_WRITE_TEST_COMPLETE(601),

    CLIENT_TEST_MISSING_OBJECT_NAME(700),
    CLIENT_TEST_BAD_MD5(701),
    CLIENT_TEST_CHECK_MD5(702),
    CLIENT_TEST_INVALID_MD5_HEADER(703),
    CLIENT_TEST_EARLY_CLOSE(704),
    CLIENT_TEST_INVALID_CONTENT_LENGTH(705),
    CLIENT_TEST_MALFORMED_REQUEST_1(706),
    CLIENT_TEST_ONE_MB_PUT(707),
    CLIENT_TEST_CREATE_BUCKET_SIMPLE(750),
    CLIENT_TEST_END(800);

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
