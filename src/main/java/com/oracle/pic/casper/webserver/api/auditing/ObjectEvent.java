package com.oracle.pic.casper.webserver.api.auditing;

public class ObjectEvent {

    public enum EventType {
        CREATE_OBJECT("CreateObject"),
        UPDATE_OBJECT("UpdateObject"),
        DELETE_OBJECT("DeleteObject");

        private final String name;

        EventType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private EventType eventType;
    private String objectName;
    private String eTag;
    private String archivalState;

    public ObjectEvent(EventType eventType, String objectName, String eTag, String archivalState) {
        this.eventType = eventType;
        this.objectName = objectName;
        this.eTag = eTag;
        this.archivalState = archivalState;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getETag() {
        return eTag;
    }

    public String getArchivalState() {
        return archivalState;
    }
}
