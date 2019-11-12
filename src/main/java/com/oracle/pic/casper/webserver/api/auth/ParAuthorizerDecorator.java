package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.tagging.client.tag.TagSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/**
 * Wrapper impl of {@link Authorizer} that performs a specific check and if that passes,
 * it then passes control to the inner delegate.
 *
 * Note that when PARs have metadata which describe an associated operation that is permissible on the object
 * being granted access to by the PAR.
 * Hence when authorizing a request, we also need to perform this specific check.
 *
 * e.g  if the PAR is only for PUT_OBJECT, then we must reject a GET operation using that PAR at runtime.
 *
 */
public class ParAuthorizerDecorator implements Authorizer {

    private static final Logger LOG = LoggerFactory.getLogger(ParAuthorizerDecorator.class);

    private final Authorizer delegate;
    private final Clock clock;

    public ParAuthorizerDecorator(Authorizer delegate, Clock clock) {
        this.delegate = delegate;
        this.clock = clock;
    }

    @Override
    public Optional<AuthorizationResponse> authorize(WSRequestContext context,
                                                     AuthenticationInfo authInfo,
                                                     @Nullable NamespaceKey namespaceKey,
                                                     @Nullable String bucketName,
                                                     String compartmentId,
                                                     @Nullable BucketPublicAccessType bucketAccessType,
                                                     CasperOperation operation,
                                                     String kmsKeyId,
                                                     boolean authorizeAll,
                                                     boolean tenancyDeleted,
                                                     CasperPermission... permissions) {
        return doAuthorization(
                context,
                authInfo,
                namespaceKey,
                bucketName,
                compartmentId,
                bucketAccessType,
                operation,
                null,
                null,
                null,
                new KmsKeyUpdateAuth(kmsKeyId, null, false),
                authorizeAll,
                tenancyDeleted,
                permissions);
    }

    @Override
    public Optional<AuthorizationResponse> authorizeWithTags(WSRequestContext context,
                                                   AuthenticationInfo authInfo,
                                                   @Nullable NamespaceKey namespaceKey,
                                                   @Nullable String bucketName,
                                                   String compartmentId,
                                                   @Nullable BucketPublicAccessType bucketAccessType,
                                                   CasperOperation operation,
                                                   @Nullable TaggingOperation taggingOperation,
                                                   @Nullable TagSet newTagSet,
                                                   @Nullable TagSet oldTagSet,
                                                   @Nullable KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                                   boolean authorizeAll,
                                                   boolean tenancyDeleted,
                                                   CasperPermission... permissions) {
        return doAuthorization(
                context,
                authInfo,
                namespaceKey,
                bucketName,
                compartmentId,
                bucketAccessType,
                operation,
                taggingOperation,
                newTagSet,
                oldTagSet,
                kmsKeyUpdateAuth,
                authorizeAll,
                tenancyDeleted,
                permissions);
    }

    private Optional<AuthorizationResponse> doAuthorization(WSRequestContext context,
                                                  AuthenticationInfo authInfo,
                                                  NamespaceKey namespaceKey,
                                                  String bucketName,
                                                  String compartmentId,
                                                  BucketPublicAccessType bucketAccessType,
                                                  CasperOperation operation,
                                                  TaggingOperation taggingOperation,
                                                  TagSet newTagSet,
                                                  TagSet oldTagSet,
                                                  @Nullable KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                                  boolean authorizeAll,
                                                  boolean tenancyDeleted,
                                                  CasperPermission... permissions) {
        final PreAuthenticatedRequestMetadata.AccessType parAccess = context.getPARAccessType().orElse(null);

        if (parAccess == null) {
            LOG.error("Failed to authorize compartment {} bucket {} for PAR operation {}",
                    compartmentId, bucketName, operation);
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespaceKey.getName()));
        }

        if (!parAccess.getCasperOps().contains(operation)) {
            LOG.debug("Cannot authorize operation {}, PAR only permits {}", operation, parAccess.getCasperOps());
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespaceKey.getName()));
        }

        final Instant date = context.getPARExpireTime().orElse(null);
        if (date == null) {
            LOG.error("Unexpected - did not find PAR expiration date in context");
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespaceKey.getName()));
        }

        if (date.isBefore(clock.instant())) {
            // PAR is expired
            LOG.debug("PAR is expired, rejecting request for authInfo: {} ");
            throw new NoSuchBucketException(BucketBackend.noSuchBucketMsg(bucketName, namespaceKey.getName()));
        }

        return delegate.authorizeWithTags(context,
                                          authInfo,
                                          namespaceKey,
                                          bucketName,
                                          compartmentId,
                                          bucketAccessType,
                                          operation,
                                          taggingOperation,
                                          newTagSet,
                                          oldTagSet,
                                          kmsKeyUpdateAuth,
                                          authorizeAll,
                                          tenancyDeleted,
                                          permissions);
    }
}
