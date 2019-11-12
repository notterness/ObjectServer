package com.oracle.pic.casper.webserver.api.logging;

import com.oracle.pic.casper.webserver.api.common.WSExceptionTranslator;

public interface ServiceLogsHandlerFactory {
    ServiceLogsHandler create(WSExceptionTranslator wsExceptionTranslator);
}
