package com.oracle.pic.casper.webserver.simulator;

import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;

/**
 * Represents an immutable traffic event for use in the traffic simulation queue.
 */

public class TrafficEvent extends Event {

    /**
     * Type of the event that will be enqueued (i.e., which methods will be called on TrafficRecorder). Includes server
     * and java events.
     */
    private final TrafficEventType type;

    /**
     * Delta bytes sent in this specific request since the last request. Always 0 for START.
     */
    private final long deltaBytes;

    /**
     * Actual rate in Megabits/second that this request was sent at to the SERVER. Always 0 for START.
     */
    private final double actualRate;

    /**
     * Original order in which event was in log, used to preserve order among updates / starts at the same time.
     */
    private final long originalIndex;


    public TrafficEvent(TrafficEventType type, String id, long time, long totalBytesReceived, long deltaBytes,
                        double actualRate, long contentLength, TrafficRecorder.RequestType reqType,
                        long originalIndex) {
        super(id, reqType, time, contentLength, totalBytesReceived);
        this.type = type;
        this.deltaBytes = deltaBytes;
        this.actualRate = actualRate;
        this.originalIndex = originalIndex;
    }

    public TrafficEventType getType() {
        return type;
    }

    public long getDeltaBytes() {
        return deltaBytes;
    }

    public double getActualRate() {
        return actualRate;
    }

    public long getOriginalIndex() {
        return originalIndex;
    }

    @Override
    /**
     * Returns String formatted with the same information that is in a Traffic Log excluding the individual bandwidth
     * estimate.
     */
    public String toString() {
        switch (this.type) {
            case START_JAVA:
                return String.format("start %s %s %d %d%n", this.getId(), this.getReqType(), this.getTime(),
                        this.getContentLength());
            case UPDATE_JAVA:
                return String.format("update %s %d %d%n", this.getId(), this.getTotalBytesReceived(), this.getTime());
            case END_JAVA:
                return String.format("end %s %d%n", this.getId(), this.getTime());
            default:
                return this.type.toString();
        }
    }

    /**
     * Outputs String formatted version of the event with most necessary information in a line json format.
     *
     * @param rateEst rate that was estimated for this event (will be the rate estimated after the last update)
     * @return json line that includes everything outputted in the log and above toString in addition to the rate
     * estimate and actual rate
     */
    public String toStringJson(double rateEst) {
        switch (this.type) {
            case START_JAVA:
                return String
                        .format("{\"type\":\"start\",\"id\":\"%s\",\"reqType\":\"%s\",\"time\":%d," +
                                        "\"contentLength\":%d,\"estimate\":%f,\"actual\":%f}%n",
                                this.getId(), this.getReqType(), this.getTime(), this.getContentLength(), rateEst,
                                this.actualRate);
            case UPDATE_JAVA:
                return String
                        .format("{\"type\":\"update\",\"id\":\"%s\",\"totalBytesReceived\":%d,\"time\":%d," +
                                        "\"estimate\":%f,\"actual\":%f}%n", this.getId(), this.getTotalBytesReceived(),
                                this.getTime(), rateEst, this.actualRate);
            case END_JAVA:
                return String.format("{\"type\":\"end\",\"id\":\"%s\",\"time\":%d,\"estimate\":%f,\"actual\":%f}%n",
                        this.getId(), this.getTime(), rateEst, actualRate);
            default:
                return this.type.toString();
        }
    }


}
