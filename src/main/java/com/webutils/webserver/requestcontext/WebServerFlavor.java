package com.webutils.webserver.requestcontext;

public enum WebServerFlavor {
    STANDARD,
    CLI_CLIENT,
    INTEGRATION_TESTS,
    INTEGRATION_OBJECT_SERVER_TEST,
    INTEGRATION_STORAGE_SERVER_TEST,
    DOCKER_OBJECT_SERVER_TEST,
    DOCKER_STORAGE_SERVER_TEST,
    DOCKER_OBJECT_SERVER_PRODUCTION,
    DOCKER_STORAGE_SERVER_PRODUCTION,
    KUBERNETES_OBJECT_SERVER_TEST,
    KUBERNETES_STORAGE_SERVER_TEST,
    INTEGRATION_DOCKER_TESTS,
    INTEGRATION_KUBERNETES_TESTS,
}
