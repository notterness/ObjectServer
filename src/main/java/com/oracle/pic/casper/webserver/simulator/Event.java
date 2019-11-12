package com.oracle.pic.casper.webserver.simulator;

import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;


/**
 * Parent class of LogEvent and TrafficEvent that has any fields that aren't specific to one or the other
 * (information needed for traffic logs, information needed for queue simulation).
 */
public class Event {

    /**
     * Unique ID of the request.
     */
    private final String id;

    /**
     * The type of the request (PutObject, GetObject, etc).
     */
    private final TrafficRecorder.RequestType reqType;

    /**
     * The time the event is logged.
     */
    private final long time;

    /**
     * Total length in bytes of the entire request.
     */
    private final long contentLength;

    /**
     * Total bytes received so far of that specific ID. All starts will have 0, all updates will be between (0,
     * contentLength], and all ends will be equal to contentLength.
     */
    private final long totalBytesReceived;

    public Event(String id, TrafficRecorder.RequestType reqType, long time, long contentLength,
                 long totalBytesReceived) {
        this.reqType = reqType;
        this.id = id;
        this.time = time;
        this.totalBytesReceived = totalBytesReceived;
        this.contentLength = contentLength;
    }

    public String getId() {
        return id;
    }

    public long getTime() {
        return time;
    }

    public long getTotalBytesReceived() {
        return totalBytesReceived;
    }

    public long getContentLength() {
        return contentLength;
    }

    public TrafficRecorder.RequestType getReqType() {
        return reqType;
    }

}

