package com.oracle.pic.casper.webserver.simulator;

import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;

/**
 * Represents an immutable Log event for use in traffic log creation.
 */

public class LogEvent extends Event {

    /**
     * Type of the event that will be enqueued; dictates behavior in simulation.
     */
    private final LogEventType type;

    public LogEvent(LogEventType type, String id, TrafficRecorder.RequestType reqType, long time, long contentLength,
                    long totalBytesReceived) {
        super(id, reqType, time, contentLength, totalBytesReceived);
        this.type = type;
    }

    @Override
    /**
     * Returns String formatted with the same information that is in a Traffic Log excluding the individual bandwidth
     * estimate.
     */
    public String toString() {
        switch (this.type) {
            case START:
                return String.format("start %s %s %d %d%n", this.getId(), this.getReqType(), this.getTime(),
                        this.getContentLength());
            case UPDATE:
                return String.format("update %s %d %d%n", this.getId(), this.getTotalBytesReceived(), this.getTime());
            case END:
                return String.format("end %s %d%n", this.getId(), this.getTime());
            default:
                return "";
        }
    }

}
