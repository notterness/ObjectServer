package com.oracle.pic.casper.webserver.auth.dataplane;

import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftCredentials;
import com.oracle.pic.casper.webserver.auth.dataplane.model.SwiftUser;

public interface SwiftCredentialsClient {
    SwiftCredentials getSwiftCredentials(SwiftUser swiftUser);
}
