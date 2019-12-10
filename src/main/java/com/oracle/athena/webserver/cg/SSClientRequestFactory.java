package com.oracle.pic.casper.webserver.api.backend.putobject.storageserver;

import com.google.common.net.HostAndPort;
import com.oracle.pic.casper.webserver.api.backend.putobject.PutObjectMachine;
import io.vertx.core.http.HttpClientRequest;

/**
 * SSRequestFactory creates Vert.x HTTP requests for storage server APIs.
 *
 * The methods of this class create HttpClientRequests with the correct URLs, query arguments, headers, hosts and ports,
 * but they must not call sendHead() or any of the handler methods on the request before returning it.
 */
public interface SSClientRequestFactory {
    HttpClientRequest putObject(HostAndPort hostAndPort, int volumeId, int von, int length);
}
