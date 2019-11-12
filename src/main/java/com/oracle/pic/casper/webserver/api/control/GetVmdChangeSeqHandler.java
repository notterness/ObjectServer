package com.oracle.pic.casper.webserver.api.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.volumemeta.VolumeMetadataClientCache;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * A Vert.x handler to handle different control plane operation.
 */
public class GetVmdChangeSeqHandler extends SyncHandler {

    private final VolumeMetadataClientCache volumeMetadataCache;
    private final ObjectMapper mapper;

    public GetVmdChangeSeqHandler(VolumeMetadataClientCache volumeMetadataCache, ObjectMapper mapper) {
        this.volumeMetadataCache = volumeMetadataCache;
        this.mapper = mapper;
    }

    @Override
    public void handleSync(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        try {
            long changeSeq = volumeMetadataCache.getVmdChangeSeq();

            HttpContentHelpers.writeJsonResponse(
                    request,
                    response,
                    changeSeq,
                    mapper);
        } catch (Exception ex) {
            throw HttpException.rewrite(request, ex);
        }
    }
}
