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
    METER_WRITE_BUFFERS(3),
    READ_BUFFER(4),
    PARSE_HTTP_BUFFER(10),
    DETERMINE_REQUEST_TYPE(11),

    CREATE_OBJECT(20),
    SETUP_OBJECT_PUT(21),
    SETUP_OBJECT_SERVER_POST(22),
    SETUP_OBJECT_GET(23),

    HANDLE_CLIENT_ERROR(50),

    OBJECT_PUT_P2(120),                 // This runs after the Object information is saved into the database
    CHECK_EMBARGO(122),                 // This is used by the AuthenticatePipelineMgr
    AUTHENTICATE_REQUEST(123),
    AUTHENTICATE_FINISHED(124),
    COMPUTE_MD5_DIGEST(130),
    COMPUTE_SHA256_DIGEST(131),
    ENCRYPT_BUFFER(135),
    STORAGE_CHUNK_ALLOC_REQUESTOR(136),
    STORAGE_CHUNK_ALLOCATED(137),
    SETUP_CHUNK_WRITE_0(140),
    SETUP_CHUNK_WRITE_1(141),
    SETUP_CHUNK_WRITE_2(142),
    CONNECT_COMPLETE(160),
    HANDLE_CHUNK_WRITE_CONN_ERROR(180),
    BUILD_HEADER_TO_STORAGE_SERVER(200),
    WRITE_HEADER_TO_STORAGE_SERVER(220),
    WRITE_TO_STORAGE_SERVER(240),
    STORAGE_SERVER_RESPONSE_BUFFER_METERING(260),
    READ_STORAGE_SERVER_RESPONSE_BUFFER(261),
    STORAGE_SERVER_RESPONSE_HANDLER(262),

    SETUP_CHUNK_READ_0(300),
    SETUP_CHUNK_READ_1(301),
    SETUP_CHUNK_READ_2(302),
    HANDLE_CHUNK_READ_CONN_ERROR(303),
    RETRIEVE_OBJECT_INFO(304),
    READ_OBJECT_CHUNKS(305),
    DECRYPT_BUFFER(306),

    SEND_FINAL_STATUS(350),
    WRITE_TO_CLIENT(351),
    REQUEST_FINISHED(352),

    STORAGE_SERVER_DETERMINE_REQUEST_TYPE(380),
    SETUP_STORAGE_SERVER_PUT(381),
    WRITE_TO_FILE(382),
    STORAGE_SERVER_SEND_FINAL_STATUS(383),

    SETUP_STORAGE_SERVER_GET(390),
    BUFFER_OBJECT_GET_METERING(391),
    READ_FROM_FILE(392),

    SETUP_CLIENT_CONNECTION(400),
    CLIENT_CONNECT_COMPLETE(401),
    CLIENT_WRITE_HTTP_HEADER(402),
    CLIENT_OBJECT_WRITE(403),
    CLIENT_WRITE(404),
    CLIENT_RESPONSE_HANDLER(405),
    HANDLE_INITIATOR_ERROR(410),

    PARSE_POST_CONTENT(450),
    CREATE_BUCKET(451),

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
    CLIENT_TEST_GET_OBJECT_SIMPLE(751),
    CLIENT_TEST_END(800);

    private final int value;

    OperationTypeEnum(final int value) {
        this.value = value;
    }

    public int toInt() {
        return this.value;
    }

    private final static Map<Integer, OperationTypeEnum> reverseLookup =
            Arrays.stream(OperationTypeEnum.values()).collect(Collectors.toMap(OperationTypeEnum::toInt, Function.identity()));

    public static OperationTypeEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_STATE);
    }
}
