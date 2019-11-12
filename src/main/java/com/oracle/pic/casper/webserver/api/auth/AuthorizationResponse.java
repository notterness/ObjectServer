package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.collect.ImmutableSet;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.webserver.api.common.TaggingClientHelper;
import com.oracle.pic.identity.authorization.permissions.Permission;
import com.oracle.pic.tagging.client.tag.TagSet;
import com.oracle.pic.tagging.common.exception.TagSetCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;

/**
 * The interface that wrap the behavior of the response of AuthZ 2.0
 */
public class AuthorizationResponse {

    private static final Logger LOG = LoggerFactory.getLogger(AuthorizationResponse.class);

    private final @Nullable TagSet tagSet;

    private final boolean authorizeTags;

    private final @Nullable String identityRequestId;

    private final Set<CasperPermission> casperPermissions;

    private final @Nullable byte[] tagSlug;

    private final @Nullable String errorMessage;

    public AuthorizationResponse(@Nullable byte[] tagSlug, @Nullable TagSet tagSet, boolean authorizeTags,
                                 Set<CasperPermission> casperPermissions) {
        this(tagSlug, tagSet, authorizeTags, null, casperPermissions, null);
    }

    public AuthorizationResponse(@Nullable byte[] tagSlug, @Nullable TagSet tagSet, boolean authorizeTags,
                                 @Nullable String identityRequestId, @Nullable String errorMessage) {
        // In the cases where we don't create the AuthorizationResponse from the SDK response,
        // just return the empty set for casperPermissions.
        this(tagSlug, tagSet, authorizeTags, identityRequestId, ImmutableSet.of(), errorMessage);
    }

    public AuthorizationResponse(@Nullable byte[] tagSlug, @Nullable TagSet tagSet, boolean authorizeTags,
                                 @Nullable String identityRequestId, Set<CasperPermission> casperPermissions,
                                 @Nullable String errorMessage) {
        this.tagSlug = tagSlug == null ? null : tagSlug.clone();
        this.tagSet = tagSet;
        this.authorizeTags = authorizeTags;
        this.identityRequestId = identityRequestId;
        this.casperPermissions = casperPermissions;
        this.errorMessage = errorMessage;
    }

    public static AuthorizationResponse fromSdkResponse(com.oracle.pic.identity.authorization.sdk.AuthorizationResponse
                                                                response) {
        final Optional<byte[]> optionalTagSlug = response.getTagSlug();
        TagSet tagSet = optionalTagSlug.map(slug -> {
                try {
                    return TaggingClientHelper.extractTagSet(slug);
                } catch (TagSetCreationException e) {
                    throw e;
                }
            }).orElse(null);

        ImmutableSet.Builder<CasperPermission> builder = ImmutableSet.builder();
        for (Permission permission : response.getPermissions()) {
            try {
                CasperPermission allowedCasperPermission = CasperPermission.valueOf(permission.getPermission());
                builder.add(allowedCasperPermission);
            } catch (Exception e) {
                LOG.debug("Could not map permission '{}' to CasperPermission. Not including it in response.",
                        permission.getPermission());
            }
        }
        builder.build();

        return new AuthorizationResponse(optionalTagSlug.orElse(null), tagSet, response.authorizeTags(),
            response.getRequestId(), builder.build(), null);
    }

    public boolean areTagsAuthorized() {
        return authorizeTags;
    }

    public Optional<TagSet> getTagSet() {
        return Optional.ofNullable(tagSet);
    }

    public Set<CasperPermission> getCasperPermissions() {
        return casperPermissions;
    }

    public String getIdentityRequestId() {
        return identityRequestId;
    }

    @Nullable
    public byte[] getTagSlug() {
        return tagSlug == null ? null : tagSlug.clone();
    }

    @Nullable
    public String getErrorMessage() {
        return errorMessage;
    }
}
