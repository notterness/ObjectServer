package com.webutils.webserver.http;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum ChunkStatusEnum {

    /*
     ** The possible Chunk Status values are:
     **
     **   INVALID - A placeholder for an invalid state and error conditions.
     **   ALLOCATED - The chunk has been allocated and is in use on a Storage Server to hold data
     **   DELETED - The chunk has been deleted (meaning the object is no longer available to the client), but it has
     **     not been cleaned up on the Storage Server yet.
     **   AVAILABLE - The chunk is available to be allocated.
     */
    INVALID_CHUNK_STATUS(0, "INVALID"),
    CHUNK_ALLOCATED(1, "ALLOCATED"),
    CHUNK_DELETED(2, "DELETED"),
    CHUNK_AVAILABLE(3, "AVAILABLE");

    private final int value;
    private final String name;

    ChunkStatusEnum(final int value, final String Name) {

        this.value = value;
        this.name = Name;
    }

    public int toInt() {
            return this.value;
        }
    public String toString() { return this.name; }

    /*
    ** The next two static functions setup the Maps that can be used to translate between an integer and the Enum and
    **   a String and its Enum.
     */
    private final static Map<Integer, com.webutils.webserver.http.ChunkStatusEnum> reverseLookup =
            Arrays.stream(com.webutils.webserver.http.ChunkStatusEnum.values()).collect(Collectors.toMap(com.webutils.webserver.http.ChunkStatusEnum::toInt, Function.identity()));

    private final static Map<String, com.webutils.webserver.http.ChunkStatusEnum> tierEnumLookup =
            Arrays.stream(com.webutils.webserver.http.ChunkStatusEnum.values()).collect(Collectors.toMap(com.webutils.webserver.http.ChunkStatusEnum::toString, Function.identity()));

    public final static com.webutils.webserver.http.ChunkStatusEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_CHUNK_STATUS);
    }

    public final static com.webutils.webserver.http.ChunkStatusEnum fromString(final String name) {
        return tierEnumLookup.getOrDefault(name, INVALID_CHUNK_STATUS);
    }

}
