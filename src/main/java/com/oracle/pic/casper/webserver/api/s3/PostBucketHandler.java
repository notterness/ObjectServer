package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.vertx.HttpServerUtil;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.ChecksumHelper;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncBodyHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoException;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.Delete;
import com.oracle.pic.casper.webserver.api.s3.model.Delete.Object;
import com.oracle.pic.casper.webserver.api.s3.model.DeleteObjectsResult;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class PostBucketHandler extends SyncBodyHandler {

    private final S3Authenticator authenticator;
    private final Backend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;

    public PostBucketHandler(S3Authenticator authenticator,
                             Backend backend,
                             XmlMapper mapper,
                             CountingHandler.MaximumContentLength maximumContentLength,
                             EmbargoV3 embargoV3) {
        super(maximumContentLength);
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public RuntimeException failureRuntimeException(int statusCode, RoutingContext context) {
        return new S3HttpException(
                S3ErrorCode.fromStatusCode(statusCode), "Entity Too Large", context.request().path());
    }

    @Override
    public void validateHeaders(RoutingContext context) {
        S3HttpHelpers.validateContentMD5ContentLengthHeaders(context.request());
    }

    @Override
    public void handleBody(RoutingContext context, byte[] bytes) {
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.DELETE_OBJECT);

        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        final List<String> parameters = HttpServerUtil.getActionParameters(request);

        if (!parameters.contains(S3Api.DELETE_PARAM)) {
            throw new NotFoundException("Delete parameter missing in request");
        }

        MetricsHandler.addMetrics(context, WebServerMetrics.S3_BULK_DELETE_BUNDLE);


        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateSha256(contentSha256, bytes, request);

        ChecksumHelper.checkContentMD5(request, bytes);

        AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);

        final Delete delete = S3HttpHelpers.parseBulkDelete(bytes, request);
        // S3 does not use conditional headers for object deletion, so we just pass null.
        final String expectedETag = null;
        final List<DeleteObjectsResult.DeletedObject> deleted = new ArrayList<>();
        final List<DeleteObjectsResult.Error> error = new ArrayList<>();

        try {
            EmbargoException savedException = null;
            for (Object object : delete.getObject()) {
                final String objectName = object.getKey();
                wsRequestContext.setObjectName(objectName);
                try {
                    try {
                        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
                            .setApi(EmbargoV3Operation.Api.S3)
                            .setOperation(CasperOperation.DELETE_OBJECT)
                            .setNamespace(namespace)
                            .setBucket(bucketName)
                            .setObject(objectName)
                            .build();
                        embargoV3.enter(embargoOperation);

                        final Optional<Date> deleteResult = backend.deleteV2Object(context, authInfo, namespace,
                            bucketName, objectName, expectedETag);
                        // S3 does not throw an error for a delete on a non-existent object,
                        // The backend logs a service entry only for successful deletes.
                        // Logging the service entry for non existent objects
                        if (!deleteResult.isPresent()) {
                            ServiceLogsHelper.logServiceEntry(context);
                        }
                        if (!delete.getQuiet()) {
                            deleted.add(new DeleteObjectsResult.DeletedObject(objectName));
                        }
                    } catch (EmbargoException e) {
                        // Do not immediately fail, we will continue to try to delete other objects
                        // (in case we have an object-name-specific embargo rule).
                        savedException = e;
                    }
                } catch (Exception ex) {
                    HttpException httpException = (HttpException) HttpException.rewrite(request, ex);
                    error.add(new DeleteObjectsResult.Error(objectName, null,
                            httpException.getErrorCode().toString(), ex.getMessage()));
                    //We need an entry for service logs in case of failure
                    ServiceLogsHelper.logServiceEntry(context, httpException.getErrorCode());
                }
            }

            if (deleted.isEmpty() && savedException != null) {
                // We did not delete any objects and at least one deletion was
                // embargoed. In this case we want to return a 503.
                //
                // If we deleted at least one object we will return that object
                // name and silently fail embargoed deletes.
                throw savedException;
            }

            DeleteObjectsResult deleteResult = new DeleteObjectsResult(deleted, error);
            S3HttpHelpers.writeXmlResponse(response, deleteResult, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
