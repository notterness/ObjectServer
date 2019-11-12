package com.oracle.pic.casper.webserver.simulator;

/**
 * For use by the TrafficRecorderSimulator as the type of TrafficEvent. SERVER events represent an arrival of the event
 * at the server on the web server, and JAVA events represent arrival at the actual Java code. This enables a pause
 * between a SERVER and JAVA event to mimic garbage collection. START, END, and UPDATE each correspond to methods to be
 * called on the TrafficRecorder.
 */
public enum TrafficEventType {
    START_SERVER,
    END_SERVER,
    UPDATE_SERVER,
    START_JAVA,
    END_JAVA,
    UPDATE_JAVA
}
