package com.oracle.pic.casper.webserver.api.healthcheck;

import com.oracle.pic.casper.common.healthcheck.HealthCheckStatus;
import com.oracle.pic.casper.common.healthcheck.HealthChecker;

public class ApiSpecificHealthChecker implements HealthChecker {

    private final HealthChecker healthChecker;
    private final String api;

    public ApiSpecificHealthChecker(HealthChecker healthChecker, String api) {
        this.healthChecker = healthChecker;
        this.api = api;
    }

    //append the api name before the message
    @Override
    public HealthCheckStatus isHealthy() {
        final HealthCheckStatus status = healthChecker.isHealthy();
        final String message = api + " " + status.getMessage();
        return new HealthCheckStatus(status.getCode(), message);
    }

    @Override
    public void declareStatus(HealthCheckStatus status) {
        healthChecker.declareStatus(status);
    }
}
