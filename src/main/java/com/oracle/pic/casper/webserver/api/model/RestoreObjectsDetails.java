package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.validation.constraints.NotNull;
import java.time.Duration;

public class RestoreObjectsDetails {
    private static final Duration TIME_TO_ARCHIVE_MIN = Duration.ofHours(1);
    private static final Duration TIME_TO_ARCHIVE_MAX = Duration.ofHours(240);

    @JsonProperty("objectName")
    @NotNull
    private final String objectName;

    @JsonProperty("hours")
    private final Duration timeToArchive;

    public RestoreObjectsDetails(@JsonProperty("objectName") String objectName,
                                 @JsonProperty("hours") Long hours) {
        this.objectName = objectName;
        if (hours != null) {
            this.timeToArchive = Duration.ofHours(hours);
        } else {
            timeToArchive = null;
        }
    }

    public String getObjectName() {
        return objectName;
    }

    public Duration getTimeToArchive() {
        return timeToArchive;
    }

    public static Duration getTimeToArchiveMin() {
        return TIME_TO_ARCHIVE_MIN;
    }

    public static Duration getTimeToArchiveMax() {
        return TIME_TO_ARCHIVE_MAX;
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        @JsonProperty("objectName")
        private String objectName;

        @JsonProperty("hours")
        private Duration timeToArchive;

        public RestoreObjectsDetails.Builder objectName(String val) {
            objectName = val;
            return this;
        }

        public RestoreObjectsDetails.Builder hours(Integer val) {
            timeToArchive = Duration.ofHours(val);
            return this;
        }

        public String getObjectName() {
            return objectName;
        }

        public Duration getTimeToArchive() {
            return timeToArchive;
        }

        public RestoreObjectsDetails build() {
            return new RestoreObjectsDetails(objectName, timeToArchive.toHours());
        }
    }
}
