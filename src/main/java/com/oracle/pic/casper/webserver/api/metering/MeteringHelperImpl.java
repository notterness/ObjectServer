package com.oracle.pic.casper.webserver.api.metering;

import com.amazonaws.util.CollectionUtils;
import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import com.oracle.pic.casper.common.config.v2.ServicePrincipalConfiguration;
import com.oracle.pic.casper.common.metering.Meter;
import com.oracle.pic.casper.common.metering.MeterWriter;
import com.oracle.pic.casper.common.metering.MeteringType;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.webserver.api.v2.CasperApiV2;
import com.oracle.pic.casper.webserver.util.TestTenants;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.identity.authentication.Principal;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper methods for writing entries to the Metering log file for either requests or bandwidth.
 */
public class MeteringHelperImpl implements MeteringHelper {

    private static Logger log = LoggerFactory.getLogger(MeteringHelperImpl.class);

    private final Clock clock;
    private final MeterWriter meterWriter;
    private final Set<String> principalTenantSet;
    private final TestTenants testTenants;

    public MeteringHelperImpl(Clock clock, MeterWriter meterWriter,
                              List<ServicePrincipalConfiguration> principalTenantSet, TestTenants testTenants) {
        this.clock = clock;
        this.meterWriter = meterWriter;
        this.principalTenantSet = !CollectionUtils.isNullOrEmpty(principalTenantSet) ?
                principalTenantSet.stream()
                        .map(ServicePrincipalConfiguration::getTenantId)
                        .collect(Collectors.toSet())
                : Collections.emptySet();
        this.testTenants = testTenants;
    }

    /***
     * Log a request to the metering log file. This method expects that the current {@link HttpServerRequest}
     * has enough information to discover the bucketName and that the {@link CommonRequestContext} has been setup.
     * @param context The request to work off of
     */
    @Override
    public void logMeteredRequest(final RoutingContext context) {
        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final CommonRequestContext commonContext = wsRequestContext.getCommonRequestContext();
        if (wsRequestContext.getPrincipal().isPresent() && isPrincipal(
                wsRequestContext.getPrincipal().get(), principalTenantSet)) {
            return;
        }
        final String opcRequestId = commonContext.getOpcRequestId();
        final String compartmentId = wsRequestContext.getCompartmentID().orElse(null);
        if (compartmentId == null) {
            log.debug("Missing compartment Id for opcRequestId {}", opcRequestId);
            WebServerMetrics.METERING_UNKNOWN.mark();
            return;
        }
        final String vcnId = wsRequestContext.getVcnID().orElse("");
        //best guess at bucket
        final String bucket = wsRequestContext.getBucketName().orElse(Meter.UNKNOWN_BUCKET_NAME);
        //soft fail the metering
        try {
            final String clientIp = getClientIP(wsRequestContext);
            Preconditions.checkNotNull(clientIp, "There is no X-Real-IP header present.");
            Meter meter = Meter.builder().applicationName(CasperApiV2.class.getSimpleName())
                    .timestamp(clock.instant())
                    .compartmentId(compartmentId)
                    .opcRequestId(opcRequestId)
                    .resourceType(MeteringType.REQUEST)
                    .value(BigDecimal.ONE)
                    .method(request.method().name())
                    .path(request.path())
                    .bucket(bucket)
                    .clientIpAddress(InetAddresses.forString(clientIp))
                    .vcnId(vcnId)
                    .build();
            meterWriter.write(meter);
            WebServerMetrics.METERING_SUCCESS.mark();
        } catch (Throwable t) {
            log.debug("Failed to meter request for opcRequestId {}", opcRequestId, t);
            WebServerMetrics.METERING_FAILURES.mark();
        }

    }

    /**
     * Log bandwidth to the metering log file. This method expects that the current {@link HttpServerRequest}
     * has enough information to discover the bucketName and that the {@link CommonRequestContext} has been setup.
     * @param context The request to work off of
     */
    @Override
    public void logMeteredBandwidth(final RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        if (wsRequestContext.getPrincipal().isPresent() && isPrincipal(
                wsRequestContext.getPrincipal().get(), principalTenantSet)) {
            return;
        }
        final CommonRequestContext commonContext = wsRequestContext.getCommonRequestContext();
        final String opcRequestId = commonContext.getOpcRequestId();
        final String compartmentId = wsRequestContext.getCompartmentID().orElse(null);
        if (compartmentId == null) {
            log.debug("Missing compartment Id for opcRequestId {}", opcRequestId);
            WebServerMetrics.METERING_UNKNOWN.mark();
            return;
        }
        final long requestBytesRead = request.bytesRead();
        final long responseBytesWritten = response.bytesWritten();
        final String vcnId = wsRequestContext.getVcnID().orElse("");
        // best guess at bucket
        final String bucket = wsRequestContext.getBucketName().orElse(Meter.UNKNOWN_BUCKET_NAME);
        final String clientIp = getClientIP(wsRequestContext);
        // soft fail the metering
        try {
            Preconditions.checkNotNull(clientIp, "There is no X-Real-IP header present.");
            final Meter.Builder meterBuilder = Meter.builder()
                    .applicationName(CasperApiV2.class.getSimpleName())
                    .opcRequestId(opcRequestId)
                    .compartmentId(compartmentId)
                    .method(request.method().name())
                    .path(request.path())
                    .bucket(bucket)
                    .clientIpAddress(InetAddresses.forString(clientIp))
                    .vcnId(vcnId);

            writeBandwidthMeter(meterBuilder, MeteringType.BANDWIDTH_INGRESS, requestBytesRead);
            WebServerMetrics.METERING_SUCCESS.mark();
            writeBandwidthMeter(meterBuilder, MeteringType.BANDWIDTH_EGRESS, responseBytesWritten);
            WebServerMetrics.METERING_SUCCESS.mark();
        } catch (Throwable t) {
            log.debug("Failed to meter bandwidth for opcRequestId {}", opcRequestId, t);
            WebServerMetrics.METERING_FAILURES.mark();
        }
    }

    private boolean isPrincipal(Principal principal, Set<String> principalTenantSet) {
        return principalTenantSet.contains(principal.getTenantId());
    }

    private void writeBandwidthMeter(Meter.Builder meterBuilder, MeteringType type, long numBytes) {
        final Meter meter = meterBuilder
                .timestamp(clock.instant())
                .resourceType(type)
                //BANDWIDTH meter has unit type DATA_TRANSFER_BYTES registered with Bling
                .value(BigDecimal.valueOf(numBytes))
                .build();
        meterWriter.write(meter);
    }

    /**
     * If request is from one of our test tenants, we can use the debug IP header if it is present
     * else use the real IP header.
     * @param requestContext
     * @return
     */
    private String getClientIP(WSRequestContext requestContext) {
        if (StringUtils.isEmpty(requestContext.getRequestorTenantOcid())) {
            log.debug("Using realIP as the clientIP for metering");
            return requestContext.getRealIP();
        }
        boolean canUseTenantIP = testTenants.tenantName(requestContext.getRequestorTenantOcid()).isPresent();
         if (canUseTenantIP && requestContext.getDebugIP().isPresent()) {
             log.debug("Using Debug IP {} for test namespace {}", requestContext.getDebugIP().get(),
                     requestContext.getNamespaceName());
             return requestContext.getDebugIP().get();
         } else return requestContext.getRealIP();
    }

}
