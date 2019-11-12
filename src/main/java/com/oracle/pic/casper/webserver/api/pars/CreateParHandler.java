package com.oracle.pic.casper.webserver.api.pars;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.CompletableBodyHandler;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.common.CompletableHandler;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.logging.ServiceLogsHelper;
import com.oracle.pic.casper.webserver.api.model.CreatePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequest;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.Validator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static javax.measure.unit.NonSI.BYTE;

/**
 * Vert.x HTTP handler for creating a PAR.
 */
public class CreateParHandler extends CompletableHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CreateParHandler.class);

    private final AsyncAuthenticator authenticator;
    private final PreAuthenticatedRequestBackend parStore;
    private final ObjectMapper mapper;
    private final CountingHandler.MaximumContentLength maximumContentLength;
    private final EmbargoV3 embargoV3;

    public CreateParHandler(AsyncAuthenticator authenticator, PreAuthenticatedRequestBackend parStore,
                            ObjectMapper mapper,
                            CountingHandler.MaximumContentLength maximumContentLength, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.parStore = parStore;
        this.mapper = mapper;
        this.maximumContentLength = maximumContentLength;
        this.embargoV3 = embargoV3;
    }

    @Override
    public CompletableFuture<Void> handleCompletably(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.CREATE_PAR);
        MetricsHandler.addMetrics(context, WebServerMetrics.V2_CREATE_PAR_BUNDLE);

        // Re-entry might not be needed, investigation here: https://jira.oci.oraclecorp.com/browse/CASPER-2807
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.getVisa().ifPresent(visa -> visa.setAllowReEntry(true));

        HttpServerRequest request = context.request();
        HttpServerResponse response = context.response();

        final String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        final String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.V2)
            .setOperation(CasperOperation.CREATE_PAR)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoV3Operation);

        Validator.validateV2Namespace(namespace);

        HttpContentHelpers.negotiateStorageObjectContent(request, 0,
                maximumContentLength.getMaximum().longValue(BYTE));

        return CompletableBodyHandler.bodyHandler(request, Buffer::getBytes)
                .thenCompose(bytes -> authenticator.authenticatePutObject(context)
                        .thenApply(authInfo -> Pair.pair(authInfo, bytes)))
                .thenCompose(authInfoAndBytes -> {
                    final Pair<CreatePreAuthenticatedRequestRequest, String> createParRequestAndUri =
                            HttpContentHelpers.readCreateParDetails(context, authInfoAndBytes.getFirst(), namespace,
                                    bucketName, authInfoAndBytes.getSecond(), mapper);
                    final CreatePreAuthenticatedRequestRequest createRequest = createParRequestAndUri.getFirst();
                    LOG.debug("Sending create PAR request to PAR backend: " + createRequest);
                    return parStore.createPreAuthenticatedRequest(createRequest)
                            .thenApply(parMd -> new PreAuthenticatedRequest(createParRequestAndUri.getSecond(),
                                        ParConversionUtils.backendToCustomerParMd(parMd)));
                })
                .handle((val, ex) -> HttpException.handle(request, val, ex))
                .thenAccept(par -> {
                    LOG.debug("Created PAR: " + par);
                    ServiceLogsHelper.logServiceEntry(context);
                    HttpContentHelpers.writeJsonResponse(request, response, par, mapper);
                });
    }
}
