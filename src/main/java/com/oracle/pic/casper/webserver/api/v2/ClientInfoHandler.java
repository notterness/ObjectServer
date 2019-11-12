package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.config.v2.BoatConfiguration;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AuthorizationException;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.AuthorizationResponse;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.ClientInfoResponse;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


/**
 * This is a <i>undocumented</i> handler exposed on the V2 API.
 *
 * It returns the server's prespective on the client's request, including:
 *
 * - The VCN ID, if any;
 * - The client's IP.
 * - The MSS of the negotiated connection
 *
 * In the future it will include additional diagnostic information, like the client's negotiated MSS.
 *
 * It only allows requests from subjectId who are authorized to perform the `GetClientInfo` operation on the Casper
 * BOAT resource compartment and the permission read object.
 *
 * The following policies are needed to perform a cross tenant request for the server info:
 * From the requesting account (replace OCIDs with appropriate for BOAT):
 *  <code>
 *      define tenancy BOAT as ocid1.tenancy.region1..aaaaaaaax7nn6kfnm2uavs6b32pslohixznn7euf4sgiqbm3of6v6hule3uq
 *      endorse group Administrators to read objects in tenancy BOAT
 *  </code>
 *
 * Within the Casper BOAT resource owner tenancy:
 * <code>
 *    admit any-group of any-tenancy to read objects in tenancy where all
 *    { request.operation='GetClientInfo', target.bucket.name = 'dev' }
 * </code>
 */
public class ClientInfoHandler extends SyncHandler {
    private final Authenticator authenticator;
    private final Authorizer authorizer;
    private final ObjectMapper objectMapper;
    private BoatConfiguration boatConfiguration;

    public ClientInfoHandler(Authenticator authenticator,
                             Authorizer authorizer,
                             ObjectMapper objectMapper,
                             BoatConfiguration boatConfiguration) {
        this.authenticator = authenticator;
        this.authorizer = authorizer;
        this.objectMapper = objectMapper;
        this.boatConfiguration = boatConfiguration;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_CLIENT_INFO);
        final HttpServerRequest request = context.request();

        try {

            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            /*
             * Restrict access to this handler by only users who have access to Casper's BOAT sparta-resources
             * compartment
             */
            final String casperBoatCompartmentOcid = boatConfiguration.getCompartmentOcid();

            /*
             * The only path this handler should be enabled on is "/n/bmcostests/b/dev/o/client"
             */
            final String namespace = "bmcostests";
            final String bucketName = "dev";

            final AuthenticationInfo authInfo = authenticator.authenticate(context);

            final Optional<AuthorizationResponse> authorizationResponse =
                    authorizer.authorize(
                            wsRequestContext,
                            authInfo,
                            new NamespaceKey(Api.V2, namespace),
                            bucketName,
                            casperBoatCompartmentOcid,
                            null,
                            CasperOperation.GET_CLIENT_INFO,
                            null,
                            true,
                            false,
                            CasperPermission.OBJECT_READ);

            if (!authorizationResponse.isPresent()) {
                throw new AuthorizationException(V2ErrorCode.UNAUTHORIZED.getStatusCode(),
                        V2ErrorCode.UNAUTHORIZED.getErrorName(), null);
            }

            final MultiMap vertxRequestHeaders = request.headers();
            final Map<String, List<String>> requestHeaders = new HashMap<>();

            vertxRequestHeaders.forEach(entry ->
                    requestHeaders.put(
                            entry.getKey().toLowerCase(),
                            vertxRequestHeaders.getAll(entry.getKey()))
            );

            final ClientInfoResponse response = new ClientInfoResponse(
                    wsRequestContext.getVcnID().orElse(null),
                    wsRequestContext.getRealIP(),
                    wsRequestContext.getMss(),
                    requestHeaders,
                    wsRequestContext.isEagleRequest()
            );

            HttpContentHelpers.writeJsonResponse(
                    request,
                    context.response(),
                    response,
                    objectMapper,
                    HttpHeaderHelpers.etagHeader(UUID.randomUUID().toString()),
                    HttpHeaderHelpers.lastModifiedHeader(DateUtil.httpFormattedDate(new Date())));

        } catch (Throwable t) {
            throw HttpException.rewrite(request, t);
        }
    }
}
