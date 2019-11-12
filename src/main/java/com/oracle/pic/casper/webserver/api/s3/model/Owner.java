package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.oracle.pic.identity.authentication.Principal;

@JsonPropertyOrder({"ID", "DisplayName"})
public class Owner {

    private final String id;
    private final String displayName;

    public Owner(Principal principal) {
        this(principal.getSubjectId(), principal.getSubjectId());
    }

    public Owner(String id, String displayName) {
        this.id = id;
        this.displayName = displayName;
    }

    @JacksonXmlProperty(localName = "ID")
    public String getID() {
        return id;
    }

    public String getDisplayName() {
        return displayName;
    }
}
