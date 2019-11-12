package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;

/**
 * RenameRequest represents an rename operation from source name to new name.
 *
 * sourceName and newName are represented as JSON objects in rename Casper API due to limitation of request PATH length.
 * For example:
 *
 * <code>
 *     {
 *         "newName": "a",
 *         "sourceName": "b",
 *         "srcObjIfMatchETag": "*",
 *         "newObjIfMatchETag": "*",
 *         "newObjIfNoneMatchETag": "*"
 *     }
 * </code>
 *
 */
@JsonDeserialize(builder = RenameRequest.Builder.class)
public class RenameRequest {
    private final String newName;
    private final String sourceName;
    private final String srcObjIfMatchETag;
    private final String newObjIfMatchETag;
    private final String newObjIfNoneMatchETag;

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        private String newName;
        private String sourceName;
        private String srcObjIfMatchETag;
        private String newObjIfMatchETag;
        private String newObjIfNoneMatchETag;

        public Builder newName(String val) {
            this.newName = val;
            return this;
        }

        public Builder sourceName(String val) {
            this.sourceName = val;
            return this;
        }

        public Builder srcObjIfMatchETag(String val) {
            this.srcObjIfMatchETag = val;
            return this;
        }

        public Builder newObjIfMatchETag(String val) {
            this.newObjIfMatchETag = val;
            return this;
        }

        public Builder newObjIfNoneMatchETag(String val) {
            this.newObjIfNoneMatchETag = val;
            return this;
        }

        public RenameRequest build() {
            Preconditions.checkNotNull(newName, "You must specify a newName");
            Preconditions.checkNotNull(sourceName, "You must specify a sourceName");
            if ("".equals(srcObjIfMatchETag)) {
                srcObjIfMatchETag = null;
            }
            if ("*".equals(srcObjIfMatchETag)) {
                srcObjIfMatchETag = null;
            }
            if ("".equals(newObjIfMatchETag)) {
                newObjIfMatchETag = null;
            }
            if ("".equals(newObjIfNoneMatchETag)) {
                newObjIfNoneMatchETag = null;
            }
            return new RenameRequest(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(RenameRequest copy) {
        Builder builder = new Builder();
        builder.newName = copy.newName;
        builder.sourceName = copy.sourceName;
        builder.srcObjIfMatchETag = copy.srcObjIfMatchETag;
        builder.newObjIfMatchETag = copy.newObjIfMatchETag;
        builder.newObjIfNoneMatchETag = copy.newObjIfNoneMatchETag;
        return builder;
    }

    public RenameRequest(Builder builder) {
        newName = builder.newName;
        sourceName = builder.sourceName;
        srcObjIfMatchETag = builder.srcObjIfMatchETag;
        newObjIfMatchETag = builder.newObjIfMatchETag;
        newObjIfNoneMatchETag = builder.newObjIfNoneMatchETag;
    }

    public String getNewName() {
        return newName;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getSrcObjIfMatchETag() {
        return srcObjIfMatchETag;
    }

    public String getNewObjIfMatchETag() {
        return newObjIfMatchETag;
    }

    public String getNewObjIfNoneMatchETag() {
        return newObjIfNoneMatchETag;
    }

    @Override
    public String toString() {
        return "RenameRequest{" +
                "newName='" + newName + '\'' +
                ", sourceName='" + sourceName + '\'' +
                ", srcObjIfMatchETag='" + srcObjIfMatchETag + '\'' +
                ", newObjIfMatchETag='" + newObjIfMatchETag + '\'' +
                ", newObjIfNoneMatchETag='" + newObjIfNoneMatchETag + '\'' +
                '}';
    }
}
