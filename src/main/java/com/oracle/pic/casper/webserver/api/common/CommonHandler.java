package com.oracle.pic.casper.webserver.api.common;

import com.google.common.collect.ImmutableSet;
import com.oracle.pic.casper.common.config.ConfigConstants;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.vertx.VertxRequestContext;
import com.oracle.pic.casper.webserver.api.sg.ServiceGateway;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.uber.jaeger.Tracer;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Operations that need to be done for every single request, period.
 *
 * This class is used for both the v1 and v2 APIs, which handle exceptions slightly differently. As a result, the
 * constructor takes an argument that determines which type of exception to throw (BadRequestException or
 * HttpException), and it throws that type of exception when a validation error occurs.
 */
public class CommonHandler implements Handler<RoutingContext> {
    private static final Pattern OPC_CLIENT_REQUEST_ID_PATTERN = Pattern.compile("^[0-9A-Za-z-/]{1,98}$");

    private final boolean useBadRequestException;
    private final Tracer tracer;
    private final ExtractsRequestMetadata requestMetadataExtractor;
    private final CasperAPI casperAPI;

    public static final Set<String> HTTP_HEADERS_CASE_SENSITIVE = ImmutableSet.of(
            "content-type",
            "content-length"
    );

    /**
    * Constructor, defaults to using HttpException for errors.
    */
    public CommonHandler(Tracer tracer, int eaglePort, CasperAPI casperAPI) {
        this(false, tracer, eaglePort, casperAPI);
    }

  /**
     * Constructor.
     *
     * @param useBadRequestException true to use BadRequestException for errors, false to use HttpException.
     * @param tracer                 the tracer to use for distributed tracing
     * @param eaglePort              the port number for eagle server
     * @param casperAPI              the Casper API that invoked this handler
     */
    public CommonHandler(boolean useBadRequestException, Tracer tracer, int eaglePort, CasperAPI casperAPI) {
        this.useBadRequestException = useBadRequestException;
        this.tracer = tracer;
        this.requestMetadataExtractor = new ExtractsRequestMetadata(eaglePort);
        this.casperAPI = casperAPI;
    }

    @Override
    public void handle(RoutingContext routingContext) {
        final HttpServerRequest request = routingContext.request();
        final HttpServerResponse response = routingContext.response();

        String vcnID;
        String clientIP;
        try {
           vcnID = requestMetadataExtractor.vcnIDFromRequest(request);
           clientIP = requestMetadataExtractor.clientIP(request);
        } catch (Throwable t) {
            if (useBadRequestException) {
                throw new BadRequestException("Not authenticated", t);
            }

            throw new HttpException(V2ErrorCode.NOT_AUTHENTICATED, request.path(), t);
        }

        final CommonRequestContext commonContext = VertxRequestContext.fromCustomerRequest(tracer, request);

        final WSRequestContext wsContext = new WSRequestContext(
                commonContext,
                casperAPI,
                vcnID,
                request.getHeader(ServiceGateway.VCN_ID_CASPER_DEBUG_HEADER),
                clientIP,
                request.getHeader(CommonHeaders.IP_ADDRESS_CASPER_DEBUG_HEADER),
                requestMetadataExtractor.mss(request),
                requestMetadataExtractor.host(request),
                requestMetadataExtractor.isEagle(request)
                );

        routingContext.put(WSRequestContext.REQUEST_CONTEXT_KEY, wsContext);
        routingContext.addHeadersEndHandler(v -> {
            putCommonHeaders(response, wsContext, casperAPI);

            /*
             * Although HTTP headers are supposed to be case-insensitive
             * (https://tools.ietf.org/html/rfc7230#section-3.2)
             * We've discovered that certain clients expect specific casing and will
             * behave oddly / fail when its not met.
             * For instance, the java aws sdk has a bug where it handles headers case sensitively
             * and a failure occurs if a header (i.e. Content-Type) is not titled.
             * It would include two headers in the signature which would fail the request.
             * https://github.com/aws/aws-sdk-java/pull/1845
             *
             * Rather than capitalize every header we'll keep a short list of headers that
             * should be capitalized.
             * {@link #HTTP_HEADERS_CASE_SENSITIVE}
             */
            MultiMap caseSensitiveMap = new CaseInsensitiveHeaders();
            for (Map.Entry<String, String> header : response.headers()) {
                if (HTTP_HEADERS_CASE_SENSITIVE.contains(header.getKey())) {
                    caseSensitiveMap.add(HttpHeaderHelpers.title(header.getKey()), header.getValue());
                } else {
                    caseSensitiveMap.add(header.getKey(), header.getValue());
                }
            }
            // replace the map with the new one
            response.headers().setAll(caseSensitiveMap);
        });

        final MetricScope metricScope = commonContext.getMetricScope();
        metricScope.annotate("isEagle", wsContext.isEagleRequest());
        metricScope.annotate("mss", wsContext.getMss());

        Optional.ofNullable(request.getHeader(HttpHeaders.USER_AGENT))
                .ifPresent(userAgent -> metricScope.annotate(HttpHeaders.USER_AGENT.toString(), userAgent));
        Optional.ofNullable(request.getHeader(CommonHeaders.OPC_CLIENT_INFO))
                .ifPresent(clientInfo -> metricScope.annotate(CommonHeaders.OPC_CLIENT_INFO.toString(), clientInfo));
        Optional.ofNullable(request.getHeader(HttpHeaders.HOST))
                .ifPresent(host -> metricScope.annotate(HttpHeaders.HOST.toString(), host));
        Optional.ofNullable(request.getHeader(HttpHeaders.CONTENT_LENGTH))
                .ifPresent(contentLen -> {
                    try {
                        metricScope.annotate(HttpHeaders.CONTENT_LENGTH.toString(), Long.parseLong(contentLen));
                    } catch (NumberFormatException e) {
                        metricScope.annotate(HttpHeaders.CONTENT_LENGTH.toString(), contentLen);
                    }
                });
        Optional.ofNullable(request.getHeader(HttpHeaders.TRANSFER_ENCODING))
                .ifPresent(encoding -> metricScope.annotate(HttpHeaders.TRANSFER_ENCODING.toString(), encoding));
        Optional.ofNullable(request.getHeader(CommonHeaders.X_FORWARDED_HOST))
                .ifPresent(originalHost ->
                        metricScope.annotate(CommonHeaders.X_FORWARDED_HOST.toString(), originalHost));

        try {
            final String opcClientRequestId = request.getHeader(CommonHeaders.OPC_CLIENT_REQUEST_ID);
            validateOpcClientRequestId(opcClientRequestId);
        } catch (IllegalArgumentException ex) {
            if (useBadRequestException) {
                throw new BadRequestException(ex.getMessage(), ex);
            }

            throw new HttpException(V2ErrorCode.INVALID_CLIENT_REQ_ID, request.path(), ex);
        }

        HttpHeaderHelpers.ensureHostHeaderParseable(request);

        MDC.put(ConfigConstants.OPC_REQUEST_ID_MDC, commonContext.getOpcRequestId());

        response.endHandler(v -> wsContext.runEndHandler(response.closed()));

        routingContext.next();

        MDC.remove(ConfigConstants.OPC_REQUEST_ID_MDC);
    }

    public static void putCommonHeaders(@Nonnull HttpServerResponse response,
                                        @Nonnull WSRequestContext context,
                                        @Nonnull CasperAPI casperAPI) {
        context.getCommonRequestContext()
                .getOpcClientRequestId()
                .ifPresent(id -> response.putHeader(CommonHeaders.OPC_CLIENT_REQUEST_ID, id));

        response.putHeader(CommonHeaders.OPC_REQUEST_ID, context.getCommonRequestContext().getOpcRequestId());
        response.putHeader(CommonHeaders.X_API_ID, casperAPI.getValue());
    }

    /**
     * Validates client request id passed in by client.
     *
     * @param opcClientRequestId the client request id
     */
    private static void validateOpcClientRequestId(@Nullable String opcClientRequestId) {
        if (opcClientRequestId != null) {
            if (!OPC_CLIENT_REQUEST_ID_PATTERN.matcher(opcClientRequestId).matches()) {
                throw new IllegalArgumentException("The '" + CommonHeaders.OPC_CLIENT_REQUEST_ID +
                                                   "' header must be characters in [A-Za-z0-9-/] " +
                                                   "and must be between 1 - 98 characters in length.");
            }
        }
    }
}
