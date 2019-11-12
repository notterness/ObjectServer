package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.time.Instant;
import java.util.List;

/**
 * This is the json object for list work requests response.
 */
public class WorkRequestJson {

    private final String operationType;
    private final String status;
    private final String id;
    private final String compartmentId;
    private final List<WorkRequestResource> resources;
    private final Instant timeAccepted;
    private final Instant timeStarted;
    private final Instant timeFinished;
    private final double percentComplete;

    public WorkRequestJson(String operationType, String status, String id, String compartmentId,
                           List<WorkRequestResource> resources, double percentComplete,
                           Instant timeAccepted, Instant timeStarted, Instant timeFinished) {
        this.operationType = operationType;
        this.status = status;
        this.id = id;
        this.compartmentId = compartmentId;
        this.resources = resources;
        this.timeAccepted = timeAccepted;
        this.timeStarted = timeStarted;
        this.timeFinished = timeFinished;
        this.percentComplete = percentComplete;
    }

    public String getOperationType() {
        return operationType;
    }

    public String getStatus() {
        return status;
    }

    public String getId() {
        return id;
    }

    public String getCompartmentId() {
        return compartmentId;
    }

    public List<WorkRequestResource> getResources() {
        return resources;
    }

    public double getPercentComplete() {
        return percentComplete;
    }

    public Instant getTimeAccepted() {
        return timeAccepted;
    }

    public Instant getTimeStarted() {
        return timeStarted;
    }

    public Instant getTimeFinished() {
        return timeFinished;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkRequestJson that = (WorkRequestJson) o;
        return  Objects.equal(operationType, that.operationType) &&
            Objects.equal(status, that.status) &&
            Objects.equal(id, that.id) &&
            Objects.equal(compartmentId, that.compartmentId) &&
            Objects.equal(resources, that.resources) &&
            Objects.equal(that.percentComplete, percentComplete) &&
            Objects.equal(timeAccepted, that.timeAccepted) &&
            Objects.equal(timeStarted, that.timeStarted) &&
            Objects.equal(timeFinished, that.timeFinished);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(operationType, status, id, compartmentId, resources, percentComplete, timeAccepted,
            timeStarted, timeFinished);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("operationType", operationType)
            .add("status", status)
            .add("id", id)
            .add("compartmentId", compartmentId)
            .add("resources", resources)
            .add("percentComplete", percentComplete)
            .add("timeAccepted", timeAccepted)
            .add("timeStarted", timeStarted)
            .add("timeFinished", timeFinished)
            .toString();
    }
}
