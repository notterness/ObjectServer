package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoContext;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.tagging.client.tag.TagSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Interface for all classes that perform authorization of HTTP requests.
 * <p>
 * Authorization has the following actors:
 * <p>
 * 1. A user, identified by an OCID, who would like to take an action. This user exists in a tenancy, and possibly
 * in a compartment within that tenancy.
 * 2. A group, identified by an OCID, in which the user has a membership.
 * 3. A compartment, identified by an OCID, which is either a root level tenancy, or a compartment that exists in a
 * tenancy (there is only one level of hierarchy currently).
 * 4. A policy, identified by an OCID, which grants the user full access to the compartment (the policy language does
 * not currently support partial access).
 * <p>
 * An implementation of this class provides a method (detailed below) that checks whether a given user has access to a
 * given compartment (the policy and group are configured through the identity service).
 */
public interface Authorizer {

    /**
     * Authorize the given user against the given compartment using the given action (verb).
     *
     * @param context          The request context, used for logging and metrics.
     * @param authInfo         The authentication information for the user making the request.
     * @param namespaceKey     Used for debugging purposes
     * @param bucketName       Used for authorization policies that match on bucket name
     * @param compartmentId    The compartment ID against which the action would be taken.
     * @param bucketAccessType Used by object operations.
     * @param operation        The name of the operation, which must match the name in the Swagger specification!
     * @param kmsKeyId         The kmsKeyId used to do kmsKeyId enforcement check.
     * @param authorizeAll     if true all provided permissions will be checked, otherwise authz will pass if any exist
     * @param permissions      The list of permissions that a user must have to do this operation.
     * @return Optional is not empty if the authorization succeeded, empty otherwise.
     */
    Optional<AuthorizationResponse> authorize(WSRequestContext context,
                                              AuthenticationInfo authInfo,
                                              @Nullable NamespaceKey namespaceKey,
                                              @Nullable String bucketName,
                                              String compartmentId,
                                              @Nullable BucketPublicAccessType bucketAccessType,
                                              CasperOperation operation,
                                              @Nullable String kmsKeyId,
                                              boolean authorizeAll,
                                              boolean tenancyDeleted,
                                              CasperPermission... permissions);

    /**
     * Authorize the given user against the given compartment using the given action (verb).
     *
     * @param authInfo         The authentication information for the user making the request.
     * @param namespaceKey     Used for debugging purposes
     * @param bucketName       Used for authorization policies that match on bucket name
     * @param compartmentId    The compartment ID against which the action would be taken.
     * @param bucketAccessType Used by object operations.
     * @param operation        The name of the operation, which must match the name in the Swagger specification!
     * @param kmsKeyId         The kmsKeyId used to do kmsKeyId enforcement check.
     * @param authorizeAll     if true all provided permissions will be checked, otherwise authz will pass if any exist
     * @param permissions      The list of permissions that a user must have to do this operation.
     * @return Optional is not empty if the authorization succeeded, empty otherwise.
     */
    Optional<AuthorizationResponse> authorize(AuthenticationInfo authInfo,
                                                     @Nullable NamespaceKey namespaceKey,
                                                     @Nullable String bucketName,
                                                     String compartmentId,
                                                     BucketPublicAccessType bucketAccessType,
                                                     CasperOperation operation,
                                                     String kmsKeyId,
                                                     boolean authorizeAll,
                                                     boolean tenancyDeleted,
                                                     MetricScope rootScope,
                                                     Set<CasperPermission> permissions,
                                                     EmbargoContext embargoContext,
                                                     String vcnId,
                                                     String vcnDebugId,
                                                     String namespace);

        /**
         * Authorize a user for one or more permissions on a compartment with the given added or changed tags.
         * @param newTagSet the new TagSet to update
         * @param oldTagSet the current TagSet of the bucket
         * @param kmsKeyUpdateAuth the kmsKeyId and it's KeyDelegationPermission, NOT null.
         * @param authorizeAll if true all provided permissions will be checked, otherwise authz will pass if any exist
         */
    Optional<AuthorizationResponse> authorizeWithTags(WSRequestContext context,
                                            AuthenticationInfo authInfo,
                                            @Nullable NamespaceKey namespaceKey,
                                            @Nullable String bucketName,
                                            String compartmentId,
                                            @Nullable BucketPublicAccessType bucketAccessType,
                                            CasperOperation operation,
                                            @Nullable TaggingOperation taggingOperation,
                                            @Nullable TagSet newTagSet,
                                            @Nullable TagSet oldTagSet,
                                            KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                            boolean authorizeAll,
                                            boolean tenancyDeleted,
                                            CasperPermission... permissions);
}
