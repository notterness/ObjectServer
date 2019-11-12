package com.oracle.pic.casper.webserver.util;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.oracle.bmc.OCID;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.encryption.exceptions.InvalidKmsKeyIdException;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.glob.GlobPattern;
import com.oracle.pic.casper.common.json.JsonSerializer;
import com.oracle.pic.casper.common.model.LifecycleEngineConstants;
import com.oracle.pic.casper.common.util.CrossRegionUtils;
import com.oracle.pic.casper.common.util.ParUtil;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.model.ObjectLifecycleRule;
import com.oracle.pic.casper.webserver.api.model.PutObjectLifecyclePolicyDetails;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidBucketIdException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidBucketNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidBucketNameLengthException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidLifecycleActionTypeException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidLifecyclePrefixException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidLifecycleRuleNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidLifecycleTimeAmountException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidLifecycleTimeUnitException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidMetadataException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidNamespaceNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidObjectNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidPatternException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidRegionException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidReplicationPolicyIdException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidReplicationPolicyNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidTagKeyException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidTagValueException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidWorkRequestIdException;
import com.oracle.pic.casper.webserver.api.model.exceptions.TagsTooLargeException;
import com.oracle.pic.casper.webserver.api.model.exceptions.TooLongObjectNameException;
import com.oracle.pic.commons.id.IdV2;
import com.oracle.pic.commons.id.InvalidOCIDException;
import com.oracle.pic.commons.id.OCIDParser;
import com.oracle.pic.tagging.client.tag.TagSet;
import com.oracle.pic.tagging.common.exception.IllegalTagKeyException;
import com.oracle.pic.tagging.common.exception.IllegalTagValueException;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Validators for namespace names, bucket names and object names.
 *
 * We do not have a validator for namespace names from the Casper V2 and Swift APIs, as the validation rules for those
 * names are managed by a separate team (and are checked when we create buckets).
 */
public final class Validator {
    static final int MAX_NAMESPACE_LENGTH = 63;
    static final int MAX_V2_NAMESPACE_LENGTH = 1024;
    static final int MAX_WORK_REQUEST_ID_LENGTH = 128;
    public static final int MAX_ID_LENGTH = 128;
    public static final int MAX_OCID_LENGTH = 1024;
    public static final int MAX_BUCKET_LENGTH = 256;
    public static final int MAX_OBJECT_LENGTH = 1024;
    public static final int MAX_METADATA_BYTES = 3500; // the Oracle row is 4000 bytes, so this is conservative.
    public static final int MAX_TAG_BYTES = 3500;
    public static final int MAX_ENCODED_STRING_LENGTH = 4000; // Max encoded string length in Oracle
    public static final int MAX_POLICY_NAME_LENGTH = 256;

    // if the string length is shorter than these numbers, we do not need to try to encode in UTF-8
    // to validate the length
    private static final int SHORT_MAX_NAMESPACE_LENGTH = MAX_NAMESPACE_LENGTH / 3;
    private static final int SHORT_MAX_V2_NAMESPACE_LENGTH = MAX_V2_NAMESPACE_LENGTH / 3;
    private static final int SHORT_MAX_BUCKET_LENGTH = MAX_BUCKET_LENGTH / 3;
    private static final int SHORT_MAX_OBJECT_LENGTH = MAX_OBJECT_LENGTH / 3;
    private static final int SHORT_MAX_POLICY_NAME_LENGTH = MAX_POLICY_NAME_LENGTH / 3;

    private static final Pattern V1_NAMESPACE = Pattern.compile("[A-Za-z0-9\\-_\\.]+");
    private static final Pattern V2_NAMESPACE = Pattern.compile("[A-Za-z0-9\\-_\\.]+");
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9\\-]+");
    private static final Pattern BUCKET = Pattern.compile("[A-Za-z0-9\\-_\\.]+");
    private static final Pattern OBJECT = Pattern.compile("[^\\r\\n\\00]+");

    /**
     * Private constructor to prevent class instantiation.
     */
    private Validator() {
    }

    /**
     * Check that the given bucket name is valid (for a request to create a bucket).
     *
     * We allow only A-Z, a-z, 0-9, -, _ and . to be part of a bucket name. The bucket name must be at least 1 character
     * and cannot be longer than 256 characters.
     *
     * @param bucket the bucket name
     * @throws InvalidBucketNameException if the bucket name is not valid.
     * @return the bucket name
     */
    public static String validateBucket(String bucket) {
        Preconditions.checkNotNull(bucket);
        if (bucket.length() >= SHORT_MAX_BUCKET_LENGTH) {
            if (getUtf8Len(bucket) > MAX_BUCKET_LENGTH) {
                throw new InvalidBucketNameLengthException("The name " + bucket + " must be between 1 and " +
                        MAX_BUCKET_LENGTH + " characters when encoded as UTF-8");
            }
        }

        if (!BUCKET.matcher(bucket).matches()) {
            throw new InvalidBucketNameException(
                    "The name " + bucket + " may contain only letters, numbers, dashes and underscores");
        }

        return bucket;
    }

    /**
     * Check that the given region name is valid.
     *
     * @param regionName the region name.
     * @return the config region.
     * @throws InvalidRegionException if the region name is not valid.
     */
    public static ConfigRegion validateRegion(String regionName) {
        Preconditions.checkNotNull(regionName);
        final ConfigRegion configRegion = CrossRegionUtils.getConfigRegion(regionName);
        if (configRegion == null) {
            throw new InvalidRegionException("Unknown region " + regionName);
        }
        return configRegion;
    }
    /**
     * Wrapper around {@link Validator#validateBucket(String)} to allow validation of internal bucket names comprising
     * a regular bucket name and a certain suffix.
     *
     * Our internal bucket name format is: namespace # InternalBucketType, thus we only need to verify
     * suffix in internalBucketName is one of the InternalBucketType.
     *
     * @param internalBucketName the internal bucket name
     * @param suffix the suffix that the bucket name should end with
     */
    public static void validateInternalBucket(String internalBucketName, String suffix) {
        if (!internalBucketName.endsWith(suffix)) {
            throw new RuntimeException("Invalid internal bucket name, expecting suffix " + suffix);
        }
    }

    /**
     * Validates whether the provided namespace name is valid or not in the V1 API (which allows upper case letters).
     *
     * We allows only A-Z, a-z, 0-9, -, _, . to be part of a namespace. The valid namespace must be at least 1 character
     * and cannot be longer than 63 characters.
     *
     * @param namespace the scope
     * @throws IllegalArgumentException if the name is invalid
     */
    public static String validateV1Namespace(String namespace) throws IllegalArgumentException {
        Preconditions.checkNotNull(namespace);
        if (namespace.length() >= SHORT_MAX_NAMESPACE_LENGTH) {
            if (getUtf8Len(namespace) > MAX_NAMESPACE_LENGTH) {
                throw new IllegalArgumentException("UTF-8 encoded namespace length must be between 1 and " +
                        MAX_NAMESPACE_LENGTH);
            }
        }

        if (!V1_NAMESPACE.matcher(namespace).matches()) {
            throw new IllegalArgumentException(
                    "The namespace (" + namespace +
                            ") must be only upper and lower case letters, numbers, dashes, underscores and periods");
        }

        return namespace;
    }

    /**
     * Validates whether the provided V2 namespace name is valid or not.
     *
     * The valid namespace must be at least 1 character and cannot be longer than 1024 characters.
     *
     * @param namespace the scope
     * @throws InvalidNamespaceNameException if the name is invalid
     */
    public static String validateV2Namespace(String namespace) throws IllegalArgumentException {
        Preconditions.checkNotNull(namespace);
        if (namespace.length() >= SHORT_MAX_V2_NAMESPACE_LENGTH) {
            if (getUtf8Len(namespace) > MAX_V2_NAMESPACE_LENGTH) {
                throw new InvalidNamespaceNameException("UTF-8 encoded namespace length must be between 1 and " +
                        MAX_V2_NAMESPACE_LENGTH);
            }
        }

        if (!V2_NAMESPACE.matcher(namespace).matches()) {
            throw new InvalidNamespaceNameException(
                    "The namespace (" + namespace +
                            ") must be only upper and lower case letters, numbers, dashes, underscores and periods");
        }
        return namespace;
    }

    /**
     * Validates whether the provided work request id is valid or not.
     *
     * The valid work request id cannot be longer than 128 characters.
     *
     * @param workRequestId the work request id to validate.
     * @throws InvalidWorkRequestIdException if the id is invalid.
     */
    public static String validateWorkRequestId(String workRequestId) throws IllegalArgumentException {
        Preconditions.checkNotNull(workRequestId);
        if (workRequestId.length() > MAX_WORK_REQUEST_ID_LENGTH) {
            throw new InvalidWorkRequestIdException("Invalid work request id");
        }
        if (!ID.matcher(workRequestId).matches()) {
            throw new InvalidWorkRequestIdException(
                    "The work request id (" + workRequestId +
                            ") must be only upper and lower case letters, numbers and dashes");
        }
        return workRequestId;
    }

    /**
     * Validates whether the provided object name is valid or not.
     *
     * The allowed name must be between 1 and 1024 in length (inclusive). Most of the characters are allowed except
     * linefeed, newline, NULL, # and ?
     *
     * @param objectName the object name
     */
    public static void validateObjectName(String objectName) {
        Preconditions.checkNotNull(objectName);
        if (objectName.length() >= SHORT_MAX_OBJECT_LENGTH) {
            if (getUtf8Len(objectName) > MAX_OBJECT_LENGTH) {
                throw new TooLongObjectNameException("UTF-8 encoded object name length must be between 1 and " +
                        MAX_OBJECT_LENGTH);
            }
        }

        if (!OBJECT.matcher(objectName).matches()) {
            throw new InvalidObjectNameException("Invalid object name: " + objectName);
        }
    }

    /**
     * Validates whether the provided string is too long.
     */
    public static boolean isUtf8EncodingTooLong(String objectName, int maxUtf8EncodedLength) {
        Preconditions.checkNotNull(objectName);
        // In the worst case a single character will encode to 3 bytes. So
        // if the string is less than 1/3 of the max length we do not have
        // to check any further.
        return (objectName.length() >= maxUtf8EncodedLength / 3) &&
                (getUtf8Len(objectName) > maxUtf8EncodedLength);
    }

    /**
     * Wrapper around {@link Validator#validateObjectName(String)} to allow validation of PAR object names (parIds).
     *
     * @param parObjectName: the par object name
     *
     */
    public static void validateParObjectName(String parObjectName) {
        // For PARs objectNames looks like
        //      - bucket level PARs -   bucketId # verifierId
        //      - object level PARs -   bucketId # parObjectName # verifierId
        int count = StringUtils.countMatches(parObjectName, ParUtil.PAR_ID_SEPARATOR_BACKEND);

        // if there are >= 2 #'s, verify the real object name
        if (count >= 2) {
            String objectName = StringUtils.substringBeforeLast(
                    StringUtils.substringAfter(parObjectName, ParUtil.PAR_ID_SEPARATOR_BACKEND),
                    ParUtil.PAR_ID_SEPARATOR_BACKEND);
            validateObjectName(objectName);
        } else if (count != 1) {
            throw new InvalidObjectNameException("Invalid PAR object name: " + parObjectName);
        }
    }

    /**
     * Validates whether the provided ObjectLifecycleDetails is valid or not.
     *
     * For each rule:
     * 1. rule name must be a valid object name (arbitrary restriction).
     * 2. timeAmount must be a non-negative int.
     * 3. timeUnit must be non-null.
     * 4. rule action must be "ARCHIVE" or "DELETE".
     * 5. rule prefixes, if they exist, must be valid object names.
     *
     * @param putObjectLifecycleDetails the list of object lifecycle rules.
     */
    public static void validateObjectLifecycleDetails(PutObjectLifecyclePolicyDetails putObjectLifecycleDetails) {
        putObjectLifecycleDetails.getItems()
                .forEach(rule -> {
                    try {
                        validateObjectName(rule.getName());
                    } catch (InvalidObjectNameException | TooLongObjectNameException e) {
                        throw new InvalidLifecycleRuleNameException("Invalid lifecycle rule name: " +
                                rule.getName());
                    }
                    if (rule.getTimeAmount() < 0) {
                        throw new InvalidLifecycleTimeAmountException("Cannot assign negative value: " +
                                rule.getTimeAmount() + " to timeAmount.");
                    }
                    if (rule.getTimeUnit() == null) {
                        throw new InvalidLifecycleTimeUnitException("TimeUnit cannot be null: ");
                    }
                    if (!rule.getAction().equalsIgnoreCase(LifecycleEngineConstants.RULE_ACTION_ARCHIVE) &&
                            !rule.getAction().equalsIgnoreCase(LifecycleEngineConstants.RULE_ACTION_DELETE)) {
                        throw new InvalidLifecycleActionTypeException("Invalid object action type, must be one of:" +
                                LifecycleEngineConstants.RULE_ACTION_ARCHIVE + ", " +
                                LifecycleEngineConstants.RULE_ACTION_DELETE);
                    }
                    if (rule.getObjectNameFilter() != null) {
                        final ObjectLifecycleRule.LifecycleRuleFilter filter = rule.getObjectNameFilter();

                        Validator.validatePatternDetails(filter.getInclusionPatterns(), filter.getExclusionPatterns());

                        if (filter.getInclusionPrefixes() != null) {
                            for (String inclusionPrefix : filter.getInclusionPrefixes()) {
                                if (inclusionPrefix == null) {
                                    throw new InvalidLifecyclePrefixException("Invalid lifecycle rule prefix: null");
                                }
                                try {
                                    validateObjectName(inclusionPrefix);
                                } catch (InvalidObjectNameException e) {
                                    throw new InvalidLifecyclePrefixException("Invalid lifecycle rule prefix: " +
                                            inclusionPrefix);
                                }
                            }

                        }
                    }
                });
    }

    /**
     * Validate inclusion and exclusion glob patterns against the PatternDetails model in the API spec.
     */
    public static void validatePatternDetails(List<String> inclusionPatterns, List<String> exclusionPatterns) {
        if (inclusionPatterns != null) {
            if (inclusionPatterns.size() > 1000) {
                throw new InvalidPatternException("At most 1000 inclusion patterns are supported");
            }
            inclusionPatterns.parallelStream().forEach(pattern -> {
                if (pattern == null) {
                    throw new InvalidPatternException("Invalid inclusion pattern: null");
                }
                if (!GlobPattern.isValidGlob(pattern)) {
                    throw new InvalidPatternException(String.format("Invalid inclusion pattern: %s", pattern));
                }
            });
        }
        if (exclusionPatterns != null) {
            if (exclusionPatterns.size() > 1000) {
                throw new InvalidPatternException("At most 1000 exclusion patterns are supported");
            }
            exclusionPatterns.parallelStream().forEach(pattern -> {
                if (pattern == null) {
                    throw new InvalidPatternException("Invalid exclusion pattern: null");
                }
                if (!GlobPattern.isValidGlob(pattern)) {
                    throw new InvalidPatternException(String.format("Invalid exclusion pattern: %s", pattern));
                }
            });
        }
        if (inclusionPatterns != null && exclusionPatterns != null &&
                !Collections.disjoint(inclusionPatterns, exclusionPatterns)) {
            throw new InvalidPatternException(String.format("Invalid patterns: inclusion and exclusion patterns " +
                    "overlap, inclusion patterns=%s, exclusion patterns=%s", inclusionPatterns, exclusionPatterns));
        }
    }

    /**
     * Wrapper around {@link Validator#validateObjectName(String)} to validate objectNamePrefix of a List PARs request.
     * when listing PARs we can list all PARs or PARs that match objectName prefixes. After converting the prefix to a
     * backend-compatible PAR ID prefix, we pass it here for validation.
     *
     * the parObjectNamePrefix looks like
     *     - without objectName prefix -   bucketId #
     *     - with objectName prefix -      bucketId # objectNamePrefix
     */
    public static void validateObjectNamePrefixForPar(String parObjectNamePrefix) {
        Preconditions.checkArgument(parObjectNamePrefix.contains(ParUtil.PAR_ID_SEPARATOR_BACKEND));
        final String objectNamePrefix =
                StringUtils.substringAfter(parObjectNamePrefix, ParUtil.PAR_ID_SEPARATOR_BACKEND);
        if (!objectNamePrefix.isEmpty()) {
            validateObjectName(objectNamePrefix);
        }
    }

    /**
     * Check that the given user-defined metadata map is valid.
     *
     * Currently validity only implies that the metadata is under the maximum size when serialized as JSON and encoded
     * as UTF-8.
     *
     * @param metadata the metadata map to check.
     * @param jsonSerializer the serializer used to convert the metadata to a JSON string
     * @return the input metadata.
     */
    public static Map<String, String> validateMetadata(Map<String, String> metadata, JsonSerializer jsonSerializer) {
        long metadataSize = jsonSerializer.toJson(metadata).getBytes(Charsets.UTF_8).length;
        if (metadataSize > MAX_METADATA_BYTES) {
            throw new InvalidMetadataException("Bucket metadata must be no more than " + MAX_METADATA_BYTES +
                    " bytes when encoded as JSON in UTF-8");
        }

        return metadata;
    }

    /**
     * Checks that the given compartmentId is a valid OCID
     *
     * @param compartmentId the compartmentId to check
     * @throws HttpException with error code 400
     * */
    public static void validateCompartmentId(@Nonnull String compartmentId) {
        try {
            OCIDParser.fromString(compartmentId);
        } catch (InvalidOCIDException ioe) {
            throw new HttpException(V2ErrorCode.BAD_REQUEST, "CompartmentId is not a valid OCID", ioe);
        }
    }

    /**
     * Check that the given freeformTags and defined tags map is valid.
     *
     * Currently validity only implies that the freeformTags (Map<String, String>) and
     * definedTags (Map<String, Map<String, String>>) is under the maximum size when serialized as JSON and encoded
     * as UTF-8.
     *
     * @param freeformTags
     * @param definedTags
     * @param jsonSerializer the serializer used to convert the tagSet's freeformTags and definedTags to a JSON string
     */
    public static void validateTags(Map<String, String> freeformTags, Map<String, Map<String, String>> definedTags,
                                    JsonSerializer jsonSerializer) {
        long freeformTagsSize = freeformTags == null ? 0 :
                jsonSerializer.toJson(freeformTags).getBytes(Charsets.UTF_8).length;
        long definedTagsSize = definedTags == null ? 0 :
                jsonSerializer.toJson(definedTags).getBytes(Charsets.UTF_8).length;
        if (freeformTagsSize + definedTagsSize > MAX_TAG_BYTES) {
            throw new TagsTooLargeException(String.format(
                    "Bucket tags must be no more than %d bytes when encoded as JSON in UTF-8", MAX_TAG_BYTES));
        }
        // try to build tagSet using tagging library which will validate each key and value.
        final TagSet.Builder tagBuilder = TagSet.builder();
        try {
            if (freeformTags != null) {
                tagBuilder.freeformTags(freeformTags);
            }
            if (definedTags != null) {
                tagBuilder.definedTags((Map) definedTags);
            }
        } catch (IllegalTagValueException ex) {
            throw new InvalidTagValueException(ex.getMessage());
        } catch (IllegalTagKeyException ex) {
            throw new InvalidTagKeyException(ex.getMessage());
        }
    }

    /**
     * Check if a string of kmsKey Ocid is validate
     * @param kmsKeyId
     */
    public static void validateKmsKeyId(String kmsKeyId) {
        IdV2.fromString(kmsKeyId).orElseThrow(() -> new InvalidKmsKeyIdException("Invalid kmsKeyId " + kmsKeyId));
    }

    /**
     * Check if a policy name is valid
     */
    public static void validatePolicyName(String policyName) {
        Preconditions.checkNotNull(policyName);
        if (policyName.isEmpty()) {
            throw new InvalidReplicationPolicyNameException("UTF-8 encoded policy name cannot be empty");
        }
        if (policyName.length() >= SHORT_MAX_POLICY_NAME_LENGTH) {
            if (getUtf8Len(policyName) > MAX_POLICY_NAME_LENGTH) {
                throw new InvalidReplicationPolicyNameException(
                    "UTF-8 encoded policy name length must be between 1 and " + MAX_POLICY_NAME_LENGTH);
            }
        }

        if (!OBJECT.matcher(policyName).matches()) {
            throw new InvalidObjectNameException("Invalid policy name: " + policyName);
        }
    }

    /**
     * Returns the modified UTF-8 encoded string length.
     *
     * @param str the string
     * @return the length of the encoded string in bytes.
     */
    private static int getUtf8Len(String str) {
        int count = 0;
        char[] charArray = str.toCharArray();
        for (int i = 0; i < charArray.length; i++) {
            char ch = charArray[i];
            if (ch <= 0x7F) {
                // ASCII character
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                // skip the surrogate
                i++;
            } else {
                count += 3;
            }
        }
        return count;
    }

    /**
     * Validates whether the provided replication policy id is valid or not.
     *
     * The valid replication policy id cannot be longer than 128 characters.
     *
     * @param replicationPolicyId the replication policy id to validate.
     * @throws InvalidReplicationPolicyIdException if the id is invalid.
     */
    public static String validateReplicationPolicyId(String replicationPolicyId) {
        Preconditions.checkNotNull(replicationPolicyId);
        if (replicationPolicyId.length() > MAX_ID_LENGTH) {
            throw new InvalidReplicationPolicyIdException("Invalid replication policy id");
        }
        if (!ID.matcher(replicationPolicyId).matches()) {
            throw new InvalidReplicationPolicyIdException(
                    "The replication policy id (" + replicationPolicyId +
                            ") must be only upper and lower case letters, numbers and dashes");
        }
        return replicationPolicyId;
    }

    /**
     * Validates whether the provided bucket id is valid or not.
     *
     * The valid bucket id cannot be longer than 128 characters.
     *
     * @param bucketId the replication policy id to validate.
     * @throws InvalidBucketIdException if the id is invalid.
     */
    public static void validateBucketId(String bucketId) {
        Preconditions.checkNotNull(bucketId);
        if (bucketId.length() > Validator.MAX_OCID_LENGTH) {
            throw new InvalidBucketIdException("Invalid bucket id");
        }
        if (!OCID.isValid(bucketId)) {
            throw new InvalidBucketIdException(
                    "The bucket id (" + bucketId + ") must be a valid OCID");
        }
    }
}
