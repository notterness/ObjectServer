package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.config.v2.BoatConfiguration;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AuthorizationException;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
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
import com.oracle.pic.casper.webserver.api.model.ServerInfoResponse;
import com.oracle.pic.casper.webserver.util.FixedJiras;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * This is a <i>undocumented</i> handler exposed on the V2 API
 * It only allows requests from subjectId who are authorized to perform the `GetServerInfo` operation on the Casper
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
 *    { request.operation='GetServerInfo', target.bucket.name = 'dev' }
 * </code>
 */
public class ServerInfoHandler extends SyncHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ServerInfoHandler.class);

    private final Authenticator authenticator;
    private final Authorizer authorizer;
    private final ObjectMapper objectMapper;
    private final BoatConfiguration boatConfiguration;

    public ServerInfoHandler(Authenticator authenticator,
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
        WSRequestContext.setOperationName(Api.V2.getVersion(), getClass(), context, CasperOperation.GET_SERVER_INFO);
        final HttpServerRequest request = context.request();

        try {

            final WSRequestContext wsRequestContext = WSRequestContext.get(context);
            /*
             * Restrict access to this handler by only users who have access to Casper's BOAT sparta-resources
             * compartment
             */
            final String casperBoatCompartmentOcid = boatConfiguration.getCompartmentOcid();

            /*
             * The only path this handler should be enabled on is "/n/bmcostests/b/dev/o/info"
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
                            CasperOperation.GET_SERVER_INFO,
                            null,
                            true,
                            false,
                            CasperPermission.OBJECT_READ);

            if (!authorizationResponse.isPresent()) {
                throw new AuthorizationException(V2ErrorCode.UNAUTHORIZED.getStatusCode(),
                        V2ErrorCode.UNAUTHORIZED.getErrorName(), null);
            }

            /*
             * Include Eagle fixed JIRAs for Eagle requests.
             */
            final List<String> fixedJiras = wsRequestContext.isEagleRequest() ?
                    FixedJiras.getFixedJiras(FixedJiras.Subset.WITH_EAGLE) :
                    FixedJiras.getFixedJiras();

            final ServerInfoResponse response = ServerInfoResponse.builder()
                    .setRegion(getEnvironmentVariable("REGION"))
                    .setStage(getEnvironmentVariable("STAGE"))
                    .setAvailabilityDomain(getEnvironmentVariable("AVAILABILITY_DOMAIN"))
                    .setApplication(getEnvironmentVariable("ODO_APPLICATION_ID"))
                    .setBuild(getEnvironmentVariable("ODO_BUILD_TAG"))
                    .setHostname(getEnvironmentVariable("HOSTNAME"))
                    .setFixedJiras(fixedJiras)
                    .build();

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

    /**
     * Get the value of an environment variable. Return "unknown" if we can not
     * read the variable or its value is null.
     */
    private static String getEnvironmentVariable(String name) {
        String value = null;
        try {
            value = System.getenv(name);
        } catch (Exception ex) {
            LOG.warn("Exception when trying to retrieve environment variable '{}'", name, ex);
        }
        if (value == null) {
            value = "unknown";
        }
        return value;
    }

}
