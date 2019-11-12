package com.oracle.pic.casper.webserver.api.v1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.model.PageableList;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.common.rest.CommonQueryParams;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Common logic for routes handling scan requests
 *
 * this class *does not* implement {@link #subclassHandle(RoutingContext)},
 * because scan requests don't need as much flexibility and don't ever need to look up an object in the MDS.
 */
abstract class AbstractScanHandler extends AbstractRouteHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractScanHandler.class);
    private final ObjectMapper objectMapper;

    AbstractScanHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Do basic request validation to catch obviously-bad requests, then proceed on to business logic in the subclass.
     */
    @Override
    protected void subclassHandle(RoutingContext routingContext) {
        validateRequest(routingContext.request());
        annotateQueryParams(routingContext);
        businessLogicCF(routingContext);
    }

    /**
     * After early request validation has succeeded, call the business logic implemented by the leaf subclass.
     *
     * Subclasses MUST override this method.
     */
    protected abstract CompletableFuture<Void> businessLogicCF(RoutingContext routingContext);

    void writePageResponse(
            HttpServerResponse response,
            List<?> resourceList,
            @Nullable String cursor,
            int requestPageSize) {
        final String json;
        try {
            json = objectMapper.writeValueAsString(resourceList);
        } catch (JsonProcessingException jpe) {
            logger.error("Could not marshal list to json", jpe);
            throw new InternalServerErrorException("Could not perform object listing", jpe);
        }
        if (cursor != null) {
            response.putHeader(CommonHeaders.OPC_NEXT_PAGE_HEADER, cursor);
        }
        Integer enforcedPageLimit = Math.min(PageableList.DEFAULT_PAGE_SIZE, requestPageSize);
        response.putHeader(CommonHeaders.OPC_LIMIT_HEADER, enforcedPageLimit.toString())
                .putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(json.getBytes(StandardCharsets.UTF_8).length))
                .write(json)
                .end();
    }

    /**
     * Given a request, make a best-effort attempt to validate it without yet looking up anything else.
     *
     * This method only catches obviously-invalid requests.  Many invalid requests cannot be identified until
     * after we know more about what the user was asking for (i.e. have made an MDS call or something).
     */
    private void validateRequest(HttpServerRequest request) {
        String limitParam = request.getParam(CommonQueryParams.LIMIT_PARAM);
        if (limitParam != null) {
            int pageSize;
            try {
                pageSize = Integer.parseInt(limitParam);
            } catch (NumberFormatException e) {
                throw new BadRequestException("Page size must be a valid integer, not '" + limitParam + "'");
            }
            if (pageSize <= 0) {
                throw new BadRequestException("Page size must be a positive integer, not '" + limitParam + "'");
            }
        }
    }

    /**
     * Annotate common query parameters onto the {@link com.oracle.pic.casper.common.metrics.MetricScope}
     */
    private void annotateQueryParams(RoutingContext routingContext) {
        HttpServerRequest request = routingContext.request();
        WSRequestContext.getCommonRequestContext(routingContext).getMetricScope()
                .annotate(CommonQueryParams.LIMIT_PARAM, request.getParam(CommonQueryParams.LIMIT_PARAM))
                .annotate(CommonQueryParams.PAGE_PARAM, request.getParam(CommonQueryParams.PAGE_PARAM));
    }
}
