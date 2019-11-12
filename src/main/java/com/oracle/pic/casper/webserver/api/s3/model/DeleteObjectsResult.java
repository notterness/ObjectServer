package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;

import java.util.List;

@JacksonXmlRootElement(localName = "DeleteResult")
public class DeleteObjectsResult extends S3XmlResult {

    @JacksonXmlProperty(localName = "Deleted")
    private List<DeletedObject> deleted;

    @JacksonXmlProperty(localName = "Error")
    private List<Error> error;

    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Iterable<DeletedObject> getDeleted() {
        return deleted;
    }

    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Iterable<Error> getError() {
        return error;
    }

    //for now we only set Key
    @JsonPropertyOrder({"Key"})
    public static final class DeletedObject {
        private String key;
        private String versionId;
        private boolean deleteMarker;
        private String deleteMarkerVersionId;

        public DeletedObject(String keyVal, String versionIdVal,
                             boolean deleteMarkerVal, String deleteMarkerVersionIdVal) {
            key = keyVal;
            versionId = versionIdVal;
            deleteMarker = deleteMarkerVal;
            deleteMarkerVersionId = deleteMarkerVersionIdVal;
        }

        public DeletedObject(String keyVal) {
            key = keyVal;
            versionId = null;
            deleteMarker = false;
            deleteMarkerVersionId = null;
        }

        /**
         * Returns the key that was successfully deleted.
         */
        @JsonProperty("Key")
        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        @JsonIgnore
        public String getVersionId() {
            return versionId;
        }
        @JsonIgnore
        public boolean getDeleteMarker() {
            return deleteMarker;
        }
        @JsonIgnore
        public String getDeleteMarkerVersionId() {
            return deleteMarkerVersionId;
        }
    }

    @JsonPropertyOrder({"Key", "VersionId", "Code", "Message"})
    public static final class Error {
        private String key;
        private String versionId;
        private String code;
        private String message;

        public Error(String keyVal, String versionVal, String codeVal, String messageVal) {
            key = keyVal;
            versionId = versionVal;
            code = codeVal;
            message = messageVal;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String getVersionId() {
            return versionId;
        }

        public String getKey() {
            return key;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }

    public DeleteObjectsResult(List<DeletedObject> deletedList, List<Error> errorList) {
        if (deletedList.isEmpty()) {
            deleted = null;
        } else {
            deleted = deletedList;
        }
        if (errorList.isEmpty()) {
            error = null;
        } else {
            error = errorList;
        }
    }
}
