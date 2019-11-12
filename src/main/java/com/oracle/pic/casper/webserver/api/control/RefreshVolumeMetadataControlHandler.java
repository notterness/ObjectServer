package com.oracle.pic.casper.webserver.api.control;

import com.oracle.pic.casper.volumemeta.VolumeMetadataClientCache;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * A Vert.x handler to handle different control plane operation.
 */
public class RefreshVolumeMetadataControlHandler extends SyncHandler  {

    private final VolumeMetadataClientCache volumeMetadataCache;

    public RefreshVolumeMetadataControlHandler(VolumeMetadataClientCache volumeMetadataCache) {
        this.volumeMetadataCache = volumeMetadataCache;
    }

    @Override
    public void handleSync(RoutingContext context) {
        volumeMetadataCache.refreshCache();
        final HttpServerResponse response = context.response();
        response.setStatusCode(204).end();
    }
}
