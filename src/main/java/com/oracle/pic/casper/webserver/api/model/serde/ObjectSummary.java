package com.oracle.pic.casper.webserver.api.model.serde;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.WSStorageObjectSummary;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ObjectSummary {

    private String name = null;
    private Long size = null;
    private Checksum checksum = null;
    private Date timeCreated = null;
    private Date timeModified = null;
    private Map<String, String> metadata = null;
    private String etag = null;
    private ArchivalState archivalState = null;

    public ObjectSummary() {

    }

    @VisibleForTesting
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public ObjectSummary(String name, Long size, Checksum checksum, Date timeCreated, Date timeModified,
                         Boolean archived, Map<String, String> metadata) {
        this.name = name;
        this.size = size;
        this.checksum = checksum;
        this.timeCreated = timeCreated;
        this.timeModified = timeModified;
        this.metadata = metadata;
    }

    @Nullable
    public static ObjectSummary makeSummary(
        List<ObjectProperties> properties, @Nullable WSStorageObjectSummary wsStorageObjectSummary,
        DecidingKeyManagementService kms) {
        if (wsStorageObjectSummary == null) return null;

        ObjectSummary objectSummary =  new ObjectSummary();
        for (ObjectProperties op: properties) {
            switch (op) {
                case NAME: objectSummary.setName(wsStorageObjectSummary.getKey().getName()); break;
                case SIZE: objectSummary.setSize(wsStorageObjectSummary.getTotalSizeInBytes()); break;
                case MD5:
                    Checksum checksum = wsStorageObjectSummary.getChecksumType() == ChecksumType.MD5 ?
                        Checksum.fromBase64(wsStorageObjectSummary.getMd5()) :
                        Checksum.fromMultipartBase64(wsStorageObjectSummary.getMd5(),
                                wsStorageObjectSummary.getPartCount());
                    objectSummary.setChecksum(checksum);
                    break;
                case TIMECREATED: objectSummary.setTimeCreated(wsStorageObjectSummary.getCreationTime()); break;
                case TIMEMODIFIED: objectSummary.setTimeModified(wsStorageObjectSummary.getModificationTime()); break;
                case METADATA: objectSummary.setMetadata(wsStorageObjectSummary.getMetadata(kms)); break;
                case ETAG: objectSummary.setETag(wsStorageObjectSummary.getETag()); break;
                case ARCHIVALSTATE: objectSummary.setArchivalState(wsStorageObjectSummary.getArchivalState()); break;
                default:
                    break;
            }
        }
        return objectSummary;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Nullable
    @JsonProperty("size")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Long getSize() {
        return size;
    }

    //Only for v2 serialization; use getChecksum() for getting md5 in code.
    @Nullable
    @JsonProperty("md5")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getMd5() {
        return Optional.ofNullable(checksum).map(c -> c.getBase64()).orElse(null);
    }

    @Nullable
    @JsonProperty("timeCreated")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DateUtil.SWAGGER_DATE_TIME_PATTERN)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Date getTimeCreated() {
        return this.timeCreated == null ? null : new Date(this.timeCreated.getTime());
    }

    @Nullable
    @JsonProperty("timeModified")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DateUtil.SWAGGER_DATE_TIME_PATTERN)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Date getTimeModified() {
        return this.timeModified == null ? null : new Date(this.timeModified.getTime());
    }

    @Nullable
    @JsonProperty("metadata")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @JsonIgnore
    public Checksum getChecksum() {
        return checksum;
    }

    @Nullable
    @JsonProperty("etag")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getETag() {
        return etag;
    }

    @Nullable
    @JsonProperty("archivalState")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ArchivalState getArchivalState() {
        return archivalState;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setChecksum(Checksum hash) {
        this.checksum = hash;
    }

    //only for deserialization in internal test client, do not call!
    public void setMd5(String md5) {
        this.checksum = Checksum.fromBase64(md5);
    }

    public void setTimeCreated(Date timeCreated) {
        this.timeCreated = new Date(timeCreated.getTime());
    }

    public void setTimeModified(Date timeModified) {
        this.timeModified = new Date(timeModified.getTime());
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public void setETag(String etag) {
        this.etag = etag;
    }

    public void setArchivalState(ArchivalState archivalState) {
        this.archivalState = archivalState;
    }
}
