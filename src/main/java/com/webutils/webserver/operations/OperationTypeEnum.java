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
    CHUNK_BUFFER_MGR_ALLOC(2),
    CHUNK_METER_BUFFERS(3),
    METER_READ_BUFFERS(4),
    BUFFER_WRITE_METERING(5),
    READ_BUFFER(6),
    PARSE_HTTP_BUFFER(10),
    DETERMINE_REQUEST(11),

    CREATE_OBJECT(20),
    SETUP_OBJECT_PUT(21),
    SETUP_CREATE_BUCKET_POST(22),
    SETUP_OBJECT_GET(23),
    SETUP_OBJECT_DELETE(24),
    SETUP_OBJECT_LIST(25),

    HANDLE_CLIENT_ERROR(50),

    OBJECT_PUT_P2(120),                 // This runs after the Object information is saved into the database
    CHECK_EMBARGO(122),                 // This is used by the AuthenticatePipelineMgr
    AUTHENTICATE_REQUEST(123),
    AUTHENTICATE_FINISHED(124),
    COMPUTE_MD5_DIGEST(130),
    COMPUTE_SHA256_DIGEST(131),
    ENCRYPT_BUFFER(135),
    WRITE_OBJECT_CHUNK(136),
    STORAGE_CHUNK_ALLOCATED(137),
    SETUP_CHUNK_WRITE_0(140),
    SETUP_CHUNK_WRITE_1(141),
    SETUP_CHUNK_WRITE_2(142),
    CONNECT_COMPLETE(160),
    HANDLE_CHUNK_WRITE_CONN_ERROR(180),
    BUILD_HEADER_TO_STORAGE_SERVER(200),
    WRITE_HEADER_TO_STORAGE_SERVER(220),
    WRITE_TO_STORAGE_SERVER(240),
    SERVICE_RESPONSE_BUFFER_METERING(260),
    READ_STORAGE_SERVER_RESPONSE_BUFFER(261),
    STORAGE_SERVER_RESPONSE_HANDLER(262),

    SETUP_CHUNK_READ_0(300),
    SETUP_CHUNK_READ_1(301),
    SETUP_CHUNK_READ_2(302),
    HANDLE_CHUNK_READ_CONN_ERROR(303),
    RETRIEVE_OBJECT_INFO(304),
    READ_OBJECT_CHUNKS(305),
    DECRYPT_BUFFER(307),
    SEND_OBJECT_GET_RESPONSE(308),
    WRITE_CHUNK_TO_CLIENT(309),
    SEND_OBJECT_DELETE_RESPONSE(310),

    SEND_FINAL_STATUS(350),
    WRITE_TO_CLIENT(351),
    CLOSE_OUT_REQUEST(352),

    /*
    ** The following Enums (starting at 380) are used for Operations within the Storage Server
     */
    STORAGE_SERVER_DETERMINE_REQUEST(380),
    SETUP_STORAGE_SERVER_PUT(381),
    SETUP_STORAGE_SERVER_GET(382),
    SETUP_STORAGE_SERVER_CHUNK_DELETE(383),
    READ_FROM_FILE(384),
    WRITE_TO_FILE(385),
    STORAGE_SERVER_SEND_FINAL_STATUS(386),
    BUFFER_OBJECT_GET_METERING(387),


    PARSE_CONTENT(450),
    CREATE_BUCKET(451),

    DATABASE_SETUP(500),

    TEST_CHUNK_WRITE(600),
    CHUNK_WRITE_TEST_COMPLETE(601),

    BUILD_REQUEST_HEADER(659),
    SEND_SERVICE_REQUEST(661),
    SERVICE_RESPONSE_HANDLER(662),

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
    CLIENT_TEST_PUT_OBJECT_SIMPLE(752),

    /*
    ** The following are all Operations that are used by the Client interface that is used to send commands to
    **   the different services. These make up the test infrastructure and the CLI interface.
     */
    BUILD_OBJECT_GET_HEADER(800),
    CLIENT_COMMAND_SEND(801),
    CLIENT_CONNECT_COMPLETE(802),
    CLIENT_GET_OBJECT(803),
    CLIENT_WRITE_HTTP_HEADER(804),
    CLIENT_OBJECT_WRITE(805),
    CLIENT_PUT_OBJECT(806),
    CLIENT_RESPONSE_HANDLER(807),
    CLIENT_WRITE(808),
    CONVERT_RESP_BODY_TO_STR(809),
    FILE_READ_BUFFER_METERING(810),
    HANDLE_INITIATOR_ERROR(811),
    OBJECT_FILE_COMPUTE_MD5(812),
    READ_OBJECT_FROM_FILE(813),
    RESPONSE_HANDLER(814),
    SETUP_CLIENT_CONNECTION(815),
    WRITE_OBJECT_TO_FILE(816),
    CLIENT_CALLBACK_OPERATION(817),


    /*
    ** The following are operations that are run as part of the ChunkMgr service
     */
    SETUP_CREATE_SERVER_POST(900),
    CREATE_SERVER(901),
    SETUP_ALLOCATE_CHUNKS_GET(902),
    ALLOCATE_CHUNKS(903),
    CHUNK_MGR_SEND_FINAL_STATUS(904),
    CHUNK_MGR_DETERMINE_REQUEST(905),
    LIST_CHUNKS(906),
    LIST_SERVERS(907),
    SETUP_LIST_CHUNKS_GET(908),
    SETUP_LIST_SERVERS_GET(909),
    SETUP_DELETE_CHUNKS(910),
    DELETE_CHUNKS(911),

    NULL_OPERATION(1000),
    CLIENT_TEST_END(1001);

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
