package com.oracle.pic.casper.webserver.api.model.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.BucketStorageTier;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.common.NamespaceCaseWhiteList;
import com.oracle.pic.casper.webserver.api.model.BucketCreate;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingBucketNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingCompartmentIdException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Helper methods to read a {@link BucketCreate} structure from UTF-8 encoded JSON bytes.
 *
 * A bucket create structure has the following JSON representation:
 *
 * <code>
 *     {
 *         "namespace": optional string, the namespace in which the bucket is stored.
 *         "name": required string, the name of the bucket.
 *         "compartmentId": required string, the compartment OCID.
 *         "metadata": optional dict with string keys and string values representing user defined metadata.
 *         "createdBy": optional string, the OCID of the user who created the bucket, ignored by this code.
 *         "createdOn": a created on date that is not settable by clients, and is ignored by this code (but it is not
 *                      an error for a client to include it).
 *         "etag": the entity tag for the bucket, which is not settable by clients and is ignored by this code (but it
 *                 is not an error for a client to include it).
 *     }
 * </code>
 *
 * See the {@link BucketCreate} class docs for details on what the individual fields mean and how they are used.
 */
public final class BucketCreateJson {

    private BucketCreateJson() {

    }

    /**
     * Read a {@link BucketCreate} object from a JSON string.
     *
     * @param namespace the namespace of the bucket to be created (usually from the URL of the request).
     * @param createdBy the OCID of the user who created the bucket.
     * @param bytes the JSON bytes, encoded as a UTF-8 string.
     * @param mapper the Jackson JSON object mapper used to deserialize the JSON bytes.
     * @throws InvalidBucketJsonException if the bucket JSON cannot be deserialized.
     * @throws MissingBucketNameException if the JSON does not contain a name.
     * @throws MissingCompartmentIdException if the JSON does not contain a compartment ID.
     * @return a well-formed BucketCreate object.
     */
    public static BucketCreate bucketCreateFromJson(
            String namespace, String createdBy, byte[] bytes, ObjectMapper mapper) {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(bytes);
        Preconditions.checkNotNull(mapper);

        final JsonNode root;
        try {
            root = mapper.readTree(bytes);
            if (root == null) {
                throw new IOException();
            }
        } catch (IOException ioex) {
            throw new InvalidBucketJsonException("The string '" + new String(bytes, Charsets.UTF_8) +
                    "' could not be parsed as a JSON object");
        }

        if (root.getNodeType() != JsonNodeType.OBJECT) {
            throw new InvalidBucketJsonException("The bucket update must be a JSON object");
        }

        Map<String, String> metadata = null;
        BucketPublicAccessType bucketPublicAccessType = BucketPublicAccessType.NoPublicAccess;
        BucketStorageTier bucketStorageTier = BucketStorageTier.Standard;
        // not assigned a default value so that PostBucketCollectionHandler knows whether objectLevelAuditMode is
        // explicitly set. After feature is public, default can be set to Disabled.
        ObjectLevelAuditMode objectLevelAuditMode = null;
        Map<String, String> freeformTags = null;
        Map<String, Map<String, Object>> definedTags = null;
        boolean objectEventsEnabled = false;

        String bucketName = null;
        String compartmentId = null;
        String kmsKeyId = null;

        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = root.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();

            BucketJsonCommon.checkNodeType(field.getKey(), field.getValue().getNodeType());
            // TODO: Delete overriding the namespace thru the body. This behavior is not supported in the official SDK.
            if (field.getKey().equals("namespace")) {
                namespace = field.getValue().asText();
                namespace = NamespaceCaseWhiteList.lowercaseNamespace(Api.V2, namespace);
            } else if (field.getKey().equals("name")) {
                bucketName = field.getValue().asText();
            } else if (field.getKey().equals("compartmentId")) {
                compartmentId = field.getValue().asText();
            } else if (field.getKey().equals("metadata")) {
                metadata = BucketJsonCommon.readMetadata(field.getValue());
            } else if (field.getKey().equals("publicAccessType") &&
                    field.getValue().getNodeType() != JsonNodeType.NULL) {
                bucketPublicAccessType = BucketPublicAccessType.fromValue(field.getValue().asText());
            } else if (field.getKey().equals("storageTier") &&
                    field.getValue().getNodeType() != JsonNodeType.NULL) {
                bucketStorageTier = BucketStorageTier.fromValue(field.getValue().asText());
            } else if (field.getKey().equals("objectLevelAuditMode") &&
                    field.getValue().getNodeType() != JsonNodeType.NULL) {
                objectLevelAuditMode = ObjectLevelAuditMode.fromValue(field.getValue().asText());
            } else if (field.getKey().equals("freeformTags")) {
                freeformTags = BucketJsonCommon.readFreeformTags(field.getValue());
            } else if (field.getKey().equals("definedTags")) {
                definedTags = BucketJsonCommon.readDefinedTags(field.getValue(), false);
            } else if (field.getKey().equals("kmsKeyId") && field.getValue().getNodeType() != JsonNodeType.NULL) {
                kmsKeyId = field.getValue().asText();
            } else if (field.getKey().equals("objectEventsEnabled")) {
                objectEventsEnabled = field.getValue().booleanValue();
            }
        }

        if (bucketName == null) {
            throw new MissingBucketNameException();
        }

        if (compartmentId == null) {
            throw new MissingCompartmentIdException();
        }

        return new BucketCreate(namespace, bucketName, compartmentId, createdBy, metadata, bucketPublicAccessType,
                bucketStorageTier, objectLevelAuditMode, freeformTags, definedTags, kmsKeyId, objectEventsEnabled);
    }
}
