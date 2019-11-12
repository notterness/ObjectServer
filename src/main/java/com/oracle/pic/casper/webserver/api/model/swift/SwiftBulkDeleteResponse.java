package com.oracle.pic.casper.webserver.api.model.swift;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

@JacksonXmlRootElement(localName = "delete")
public class SwiftBulkDeleteResponse {

    @JsonProperty("number_deleted")
    private final int numberDeleted;

    @JsonProperty("number_not_found")
    private final int numberNotFound;

    public static final class ErrorObject {

        @JsonProperty("name")
        private final String name;

        @JsonProperty("status")
        private final String status;

        public ErrorObject(String name, String status) {
            this.name = name;
            this.status = status;
        }

        public String getName() {
            return name;
        }

        public String getStatus() {
            return status;
        }
    }

    @JacksonXmlElementWrapper(localName = "errors")
    @JacksonXmlProperty(localName = "object")
    private final List<ErrorObject> errors;

    public SwiftBulkDeleteResponse(int numberDeleted, int numberNotFound, List<ErrorObject> errors) {
        this.numberDeleted = numberDeleted;
        this.numberNotFound = numberNotFound;
        this.errors = errors;
    }

    public int getNumberDeleted() {
        return numberDeleted;
    }

    public int getNumberNotFound() {
        return numberNotFound;
    }

    public List<ErrorObject> getErrors() {
        return errors;
    }
}
