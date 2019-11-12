package com.oracle.pic.casper.webserver.api.model;

import com.google.common.collect.ImmutableList;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.exceptions.CasperException;
import com.oracle.pic.casper.workrequest.WorkRequest;
import com.oracle.pic.casper.workrequest.WorkRequestDetail;
import com.oracle.pic.casper.workrequest.WorkRequestType;
import com.oracle.pic.casper.workrequest.bulkrestore.BulkRestoreRequestDetail;
import com.oracle.pic.casper.workrequest.copy.CopyRequestDetail;
import com.oracle.pic.casper.workrequest.reencrypt.ReencryptBucketRequestDetail;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Helper class to create WorkRequestJson for get/list work request responses.
 */
public final class WorkRequestJsonHelper {

    public static final Map<WorkRequestType, Class<? extends WorkRequestDetail>> WORK_REQUEST_DETAIL_CLASS =
            ImmutableMap.of(
            WorkRequestType.COPY, CopyRequestDetail.class,
            WorkRequestType.BULK_RESTORE, BulkRestoreRequestDetail.class,
            WorkRequestType.REENCRYPT, ReencryptBucketRequestDetail.class);
    private WorkRequestJsonHelper() {
    }

    /**
     * If region is not null and region is not current region, use the full URL
     */
    static String objectUri(String namespace, String bucket, String object, @Nullable ConfigRegion region) {
        final boolean isCrossRegion = region != null && ConfigRegion.fromSystemProperty() != region;
        try {
            return (isCrossRegion ? region.getEndpoint() : "") +
                "/n/" + URLEncoder.encode(namespace, StandardCharsets.UTF_8.name()) +
                "/b/" + URLEncoder.encode(bucket, StandardCharsets.UTF_8.name()) +
                "/o/" + URLEncoder.encode(object, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    static String bucketUri(String namespace, String bucket, @Nullable ConfigRegion region) {
        final boolean isCrossRegion = region != null && ConfigRegion.fromSystemProperty() != region;
        try {
            return (isCrossRegion ? region.getEndpoint() : "") +
                    "/n/" + URLEncoder.encode(namespace, StandardCharsets.UTF_8.name()) +
                    "/b/" + URLEncoder.encode(bucket, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<WorkRequestResource> copyRequestDetailToResource(CopyRequestDetail detail) {
        final WorkRequestResource.Metadata sourceMetadata = new WorkRequestResource.Metadata(
            ConfigRegion.fromSystemProperty().getFullName(),
            detail.getSourceNamespace(),
            detail.getSourceBucket(),
            detail.getSourceObject());
        final WorkRequestResource source = WorkRequestResource.builder()
            .entityType("object")
            .actionType(WorkRequestResource.ActionType.Read)
            .entityUri(objectUri(detail.getSourceNamespace(), detail.getSourceBucket(), detail.getSourceObject(), null))
            .metadata(sourceMetadata)
            .build();
        final WorkRequestResource.Metadata destMetadata = new WorkRequestResource.Metadata(
            detail.getDestConfigRegion().getFullName(),
            detail.getDestNamespace(),
            detail.getDestBucket(),
            detail.getDestObject());
        final WorkRequestResource dest = WorkRequestResource.builder()
            .entityType("object")
            .actionType(WorkRequestResource.ActionType.Written)
            .entityUri(objectUri(detail.getDestNamespace(), detail.getDestBucket(), detail.getDestObject(),
                detail.getDestConfigRegion()))
            .metadata(destMetadata)
            .build();
        return ImmutableList.of(source, dest);
    }

    public static List<WorkRequestResource> workRequestResourcesFromDetail(WorkRequestDetail detail,
                                                                            WorkRequest workRequest) {
        if (workRequest.getType() == WorkRequestType.COPY) {
            return copyRequestDetailToResource((CopyRequestDetail) detail);
        } else if (workRequest.getType() == WorkRequestType.REENCRYPT) {
            return reencryptRequestDetailToResource((ReencryptBucketRequestDetail) detail, workRequest);
        } else if (workRequest.getType() == WorkRequestType.BULK_RESTORE) {
            return null;
        } else {
            throw new CasperException("Invalid WorkRequestType type " + workRequest.getType().toString());
        }
    }

    private static List<WorkRequestResource> reencryptRequestDetailToResource(ReencryptBucketRequestDetail detail,
                                                                              WorkRequest workRequest) {
        final WorkRequestResource.ActionType actionType;
        if (workRequest.getState().isEndState()) {
            actionType = WorkRequestResource.ActionType.Updated;
        } else {
            actionType = WorkRequestResource.ActionType.InProgress;
        }
        final WorkRequestResource.Metadata metadata = new WorkRequestResource.Metadata(
                ConfigRegion.fromSystemProperty().getFullName(),
                detail.getNamespace(),
                detail.getBucket(),
                "");
        final WorkRequestResource resource = WorkRequestResource.builder()
                .entityType("bucket")
                .actionType(actionType)
                .entityUri(bucketUri(detail.getNamespace(), detail.getBucket(), null))
                .metadata(metadata)
                .build();
        return ImmutableList.of(resource);
    }

}
