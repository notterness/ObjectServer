package com.oracle.pic.casper.webserver.simulator;

/**
 * For use by the TrafficRecorderSimulator as the type of TrafficEvent. Corresponds to the three things a request/client
 * can
 * send.
 */
public enum LogEventType {
    START,
    UPDATE,
    END
}
