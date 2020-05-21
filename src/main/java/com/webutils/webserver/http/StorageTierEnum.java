package com.webutils.webserver.http;

import com.webutils.webserver.operations.OperationTypeEnum;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum StorageTierEnum {

    INVALID_TIER(0, "invalid", 0),
    STANDARD_TIER(1, "Standard", 3),
    INTELLIGENT_TIER(2, "Intelligent-Tiering", 3),
    STANDARD_INFREQUENT_ACCESS_TIER(3, "Standard-IA", 3),
    ONE_ZONE_TIER(4, "OneZone", 2),
    ARCHIVE_TIER(5, "Archive", 3),
    DEEP_ARCHIVE_TIER(6, "DeepArchive", 3);


    private final int value;
    private final String tierName;
    private final int redundancy;

    StorageTierEnum(final int value, final String tierName, final int redundancy) {

        this.value = value;
        this.tierName = tierName;
        this.redundancy = redundancy;
    }

    public int toInt() {
        return this.value;
    }

    public String toString() { return this.tierName; }

    public int getRedundancy() { return this.redundancy; }

    /*
    ** The next two static functions setup the Maps that can be used to translate between an integer and the Enum and
    **   a String and its Enum.
     */
    private final static Map<Integer, StorageTierEnum> reverseLookup =
            Arrays.stream(StorageTierEnum.values()).collect(Collectors.toMap(StorageTierEnum::toInt, Function.identity()));

    private final static Map<String, StorageTierEnum> tierEnumLookup =
            Arrays.stream(StorageTierEnum.values()).collect(Collectors.toMap(StorageTierEnum::toString, Function.identity()));

    public final static StorageTierEnum fromInt(final int id) {
        return reverseLookup.getOrDefault(id, INVALID_TIER);
    }

    public final static StorageTierEnum fromString(final String tierName) {
        return tierEnumLookup.getOrDefault(tierName, INVALID_TIER);
    }
}
