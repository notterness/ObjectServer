package com.oracle.athena.webserver.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class GlobalSystemPropertiesConfigurator {
    private static final Logger logger = LoggerFactory.getLogger(GlobalSystemPropertiesConfigurator.class);
    private static volatile boolean configured = false;

    private GlobalSystemPropertiesConfigurator() {
    }

    public static void configure() {
        if (!configured) {
            // FIXME: This needs to be removed once we've fully de-vert.xed
            System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
            registerSlf4jBridge();
            configured = true;
        }
    }

    private static void registerSlf4jBridge() {
        try {
            Class<?> julBridge = Class.forName("org.slf4j.bridge.SLF4JBridgeHandler");
            Method install = julBridge.getDeclaredMethod("install");
            install.invoke(new Object[0]);
        } catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException | NoSuchMethodException var2) {
            logger.error("Error while registering Slf4j bridge for java.util.logging", var2);
        }

    }
}
