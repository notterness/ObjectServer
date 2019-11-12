package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.lifecycle.LifecycleEngineClient;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.backend.BucketBackend;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpMatchHelpers;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * Vert.x HTTP handler to delete a lifecycle policy.
 *
 * This handler is simple compared to the {@link PutLifecyclePolicyHandler}.  It only:
 * 1. Authenticates the user
 * 2. Cancels any ongoing execution of the previous policy on the user's target bucket
 * 3. Updates the target bucket's lifecycle policy ETag to null
 * 4. Returns 204 No Content
 *
 * This handler does not currently clean up old lifecycle policy objects.  When you delete (or overwrite) a lifecycle
 * policy, the object that stored the old policy is simply leaked.  See
 * https://jira.oci.oraclecorp.com/browse/CASPER-4668
 */
public class DeleteLifecyclePolicyHandler extends SyncHandler {

    private final Authenticator authenticator;
    private final BucketBackend bucketBackend;
    private final LifecycleEngineClient lifecycleEngineClient;
    private final EmbargoV3 embargoV3;

    public DeleteLifecyclePolicyHandler(Authenticator authenticator, BucketBackend bucketBackend,
                                        LifecycleEngineClient lifecycleEngineClient, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.bucketBackend = bucketBackend;
        this.lifecycleEngineClient = lifecycleEngineClient;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.UPDATE_BUCKET);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_DELETE_LIFECYCLE_BUNDLE);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, WSRequestContext.get(context));

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.UPDATE_BUCKET)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        Validator.validateV2Namespace(namespace);
        HttpMatchHelpers.validateConditionalHeaders(request, HttpMatchHelpers.IfMatchAllowed.YES_WITH_STAR,
                HttpMatchHelpers.IfNoneMatchAllowed.NO);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context);

            // Cancel lifecycle policy in lifecycle engine
            lifecycleEngineClient.cancel(namespace, bucketName);

            // We delete lifecycle policy by updating bucket info as "CLEAR_LIFECYCLE_POLICY", lifecycle object
            // will remain until garbage collector recycle it.
            bucketBackend.deleteBucketCurrentLifecyclePolicyEtag(context, authInfo, namespace, bucketName);

            response.setStatusCode(HttpResponseStatus.NO_CONTENT).end();
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
