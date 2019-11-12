package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.collect.ImmutableMultimap;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps the public access type to the set of casper operations that are made public. Provides utility to authenticate
 * anonymous user and check to see if the bucket (if exists) is made public for the operation, such that it can accept
 * request from anonymous or unauthenticated user.
 */
public final class PublicOperations {

    private static final Logger LOG = LoggerFactory.getLogger(PublicOperations.class);

    private PublicOperations() {

    }

    private static final ImmutableMultimap<BucketPublicAccessType, CasperOperation> PUBLIC_OPERATION_MAP;
    static {
        PUBLIC_OPERATION_MAP = ImmutableMultimap.<BucketPublicAccessType, CasperOperation>builder()
                .put(BucketPublicAccessType.ObjectRead, CasperOperation.GET_OBJECT)
                .put(BucketPublicAccessType.ObjectRead, CasperOperation.HEAD_OBJECT)
                .put(BucketPublicAccessType.ObjectRead, CasperOperation.LIST_OBJECTS)
                .put(BucketPublicAccessType.ObjectReadWithoutList, CasperOperation.GET_OBJECT)
                .put(BucketPublicAccessType.ObjectReadWithoutList, CasperOperation.HEAD_OBJECT)
                .build();
    }

    /**
     * Returns true if the bucket allows public access for the given operation and access type.
     */
    private static boolean isPublicAccessEnabled(CasperOperation operation,
            BucketPublicAccessType publicAccessType) {
        return PUBLIC_OPERATION_MAP.containsKey(publicAccessType) &&
                PUBLIC_OPERATION_MAP.get(publicAccessType).contains(operation);
    }

    public static boolean bucketAllowsPublicAccess(
            CasperOperation operation,
            NamespaceKey namespaceKey,
            String bucketName,
            BucketPublicAccessType accessType) {
        if (isPublicAccessEnabled(operation, accessType)) {
            LOG.debug("Allowing {} on bucket {} in namespace {} since it is marked as public by {}",
                    operation,
                    bucketName,
                    namespaceKey,
                    accessType);
            return true;
        }
        return false;
    }
}
