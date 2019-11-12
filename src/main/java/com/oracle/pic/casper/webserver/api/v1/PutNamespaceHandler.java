package com.oracle.pic.casper.webserver.api.v1;

import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.BucketKey;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.objectmeta.TenantDb;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.backend.TenantBackend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.BucketCreate;
import com.oracle.pic.casper.webserver.api.model.exceptions.BucketAlreadyExistsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidBucketNameException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

/**
 * Saves a namespace to the metadata store
 *
 * Note:  this class does not extend either of the {@link AbstractRouteHandler} subclasses, this class extends it
 * directly.  This handler just doesn't share any code in common with scanning or dealing with objects.
 */
public class PutNamespaceHandler extends AbstractRouteHandler {

    private final BucketBackend bucketBackend;
    private final TenantBackend tenantBackend;
    private final EmbargoV3 embargoV3;

    public PutNamespaceHandler(
        BucketBackend bucketBackend, TenantBackend tenantBackend, EmbargoV3 embargoV3) {
        this.bucketBackend = bucketBackend;
        this.tenantBackend = tenantBackend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void subclassHandle(RoutingContext routingContext) {
        final WSRequestContext wsRequestContext = routingContext.get(WSRequestContext.REQUEST_CONTEXT_KEY);
        final HttpServerRequest request = routingContext.request();
        wsRequestContext.setNamespaceName(RequestHelpers.computeNamespaceKey(request).getName());
        final MetricScope parentScope = wsRequestContext.getCommonRequestContext().getMetricScope();

        VertxUtil.runAsync(parentScope.child("namespaceDb:putBucket"), () -> this.mdsPutNamespace(routingContext))
                .thenRun(() -> routingContext.response().end())
                .exceptionally(throwable -> fail(routingContext, throwable));
    }

    private Void mdsPutNamespace(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        MetricsHandler.addMetrics(context, WebServerMetrics.V1_PUT_NS_BUNDLE);
        final MetricScope scope = commonContext.getMetricScope();

        final BucketKey bucketKey = RequestHelpers.computeBucketKey(request).withBucketId();
        try {
            Validator.validateBucket(bucketKey.getName());
        } catch (InvalidBucketNameException ex) {
            throw new BadRequestException(ex.getMessage());
        }

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V1)
            .setNamespace(bucketKey.getNamespace())
            .setBucket(bucketKey.getName())
            .setOperation(CasperOperation.CREATE_BUCKET)
            .build();
        embargoV3.enter(embargoV3Operation);

        // Create the namespace with the tenant ocid associated with the namespace.
        tenantBackend.getOrCreateNamespace(context, scope,
            new NamespaceKey(Api.V1, bucketKey.getNamespace()),
            AuthenticationInfo.V1_USER.getMainPrincipal().getTenantId());
        try {
            final BucketCreate bucketCreate = new BucketCreate(bucketKey.getNamespace(),
                    bucketKey.getName(),
                    TenantDb.V1_COMPARTMENT_ID,
                    TenantDb.V1_CREATION_USER,
                    ImmutableMap.of(),
                    BucketPublicAccessType.ObjectRead);
            bucketBackend.createBucket(context, AuthenticationInfo.V1_USER, bucketCreate, Api.V1);
            return null;
        } catch (BucketAlreadyExistsException e) {
            // The bucket already exists so no need to take any action for V1 API
            return null;
        }
    }
}
