package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/*
 * Since S3 uses the same path with different parameters for different operations,
 * this handler directs it to the right handler.
 */
public class S3ObjectPathHandler implements Handler<RoutingContext> {

    private final PutObjectHandler putObjectHandler;
    private final HeadObjectHandler headObjectHandler;
    private final GetObjectHandler getObjectHandler;
    private final DeleteObjectHandler deleteObjectHandler;

    private final CreateUploadHandler createUploadHandler;
    private final AbortUploadHandler abortUploadHandler;
    private final CommitUploadHandler commitUploadHandler;
    private final ListUploadPartsHandler listUploadPartsHandler;
    private final PutPartHandler putPartHandler;
    private final RestoreObjectsHandler restoreObjectsHandler;
    private final UpdateMetadataHandler updateMetadataHandler;
    private final CopyObjectHandler copyObjectHandler;
    private final CopyPartHandler copyPartHandler;

    public S3ObjectPathHandler(PutObjectHandler putObjectHandler,
                               HeadObjectHandler headObjectHandler,
                               GetObjectHandler getObjectHandler,
                               DeleteObjectHandler deleteObjectHandler,
                               CreateUploadHandler createUploadHandler,
                               AbortUploadHandler abortUploadHandler,
                               CommitUploadHandler commitUploadHandler,
                               ListUploadPartsHandler listUploadPartsHandler,
                               PutPartHandler putPartHandler,
                               RestoreObjectsHandler restoreObjectsHandler,
                               UpdateMetadataHandler updateMetadataHandler,
                               CopyObjectHandler copyObjectHandler,
                               CopyPartHandler copyPartHandler) {
        this.putObjectHandler = putObjectHandler;
        this.headObjectHandler = headObjectHandler;
        this.getObjectHandler = getObjectHandler;
        this.deleteObjectHandler = deleteObjectHandler;
        this.createUploadHandler = createUploadHandler;
        this.abortUploadHandler = abortUploadHandler;
        this.commitUploadHandler = commitUploadHandler;
        this.listUploadPartsHandler = listUploadPartsHandler;
        this.putPartHandler = putPartHandler;
        this.restoreObjectsHandler = restoreObjectsHandler;
        this.updateMetadataHandler = updateMetadataHandler;
        this.copyObjectHandler = copyObjectHandler;
        this.copyPartHandler = copyPartHandler;
    }

    @Override
    public void handle(RoutingContext context) {
        WSRequestContext.setOperationName("s3", getClass(), context, null);

        final HttpServerRequest request = context.request();
        S3HttpHelpers.getNamespace(request, WSRequestContext.get(context));
        final HttpMethod method = request.method();
        switch (method) {
            case PUT:
                if (request.params().contains(S3Api.UPLOADID_PARAM)) {
                    if (request.getHeader(S3Api.COPY_SOURCE_HEADER) != null) {
                        copyPartHandler.handle(context);
                    } else {
                        putPartHandler.handle(context);
                    }
                } else if (request.getHeader(S3Api.COPY_SOURCE_HEADER) != null) {
                    if (isUpdateMetadataRequest(context)) {
                        updateMetadataHandler.handle(context);
                    } else {
                        copyObjectHandler.handle(context);
                    }
                } else {
                    putObjectHandler.handle(context);
                }
                break;
            case HEAD:
                headObjectHandler.handle(context);
                break;
            case GET:
                if (request.params().contains(S3Api.UPLOADID_PARAM)) {
                    listUploadPartsHandler.handle(context);
                } else {
                    getObjectHandler.handle(context);
                }
                break;
            case DELETE:
                if (request.params().contains(S3Api.UPLOADID_PARAM)) {
                    abortUploadHandler.handle(context);
                } else {
                    deleteObjectHandler.handle(context);
                }
                break;
            case POST:
                if (request.params().contains(S3Api.UPLOADS_PARAM)) {
                    createUploadHandler.handle(context);
                    break;
                } else if (request.params().contains(S3Api.UPLOADID_PARAM)) {
                    commitUploadHandler.handle(context);
                    break;
                } else if (request.params().contains(S3Api.RESTORE_PARAM)) {
                    restoreObjectsHandler.handle(context);
                    break;
                }
            default:
                throw new NotImplementedException();
        }
    }

    /**
     * Checks if the request is an update metadata request
     * @param context routing context
     * @return true for an update metadata request, false otherwise
     */
    private boolean isUpdateMetadataRequest(RoutingContext context) {
        try {
            final HttpServerRequest request = context.request();
            final String copySource = S3HttpHelpers.unquoteIfNeeded(request.getHeader(S3Api.COPY_SOURCE_HEADER));
            final String splitCopySource = copySource.startsWith("/") ? copySource.substring(1) : copySource;
            final String decodeSource = URLDecoder.decode(splitCopySource, StandardCharsets.UTF_8.name());

            final String[] source = decodeSource.split("/", 2);
            if (source.length != 2) {
                throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                        "x-amz-copy-source is not specified correctly. it should be source_bucket/object",
                        // S3 SDK encodes double slash string ("//") as "/%2F", thus we turn it back.
                        request.path().replace("/%2F", "//"));
            }
            final String sourceBucket = source[0];
            final String sourceObject = source[1];

            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            final String destinationObject = S3HttpHelpers.getObjectName(request, wsRequestContext);
            final String destinationBucket = S3HttpHelpers.getBucketName(request, wsRequestContext);

            return sourceBucket.equals(destinationBucket) && sourceObject.equals(destinationObject);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }
}
