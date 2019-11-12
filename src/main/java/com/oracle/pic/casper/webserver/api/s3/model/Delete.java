package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.List;

public class Delete {
    public static final class Object {
        @JacksonXmlProperty(localName = "VersionId")
        private String versionId;

        @JacksonXmlProperty(localName = "Key")
        private String key;

        public Object() {
            versionId = null;
            key = null;
        }

        public Object(String versionIdVal, String keyVal) {
            versionId = versionIdVal;
            key = keyVal;
        }

        public String getVersionId() {
            return versionId;
        }

        public void setVersionId(String versionIdVal) {
            versionId = versionIdVal;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String keyVal) {
            key = keyVal;
        }

    }
    @JacksonXmlProperty(localName = "Quiet")
    private boolean quiet;

    public boolean getQuiet() {
        return quiet;
    }

    public void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }

    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Object> object;

    public List<Object> getObject() {
        return object;
    }

    public void setObject(List<Object> listObjects) {
        object = listObjects;
    }
}
