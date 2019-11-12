package com.oracle.pic.casper.webserver.api.eventing;

import com.oracle.pic.casper.common.clients.FakeAuthDetailsProvider;
import com.oracle.pic.events.EventsIngestionClient;
import com.oracle.pic.events.model.BaseEvent;
import com.oracle.pic.events.model.EventV01;
import com.oracle.pic.events.model.FailedEventRecord;
import com.oracle.pic.events.model.PublishEventsResponseDetails;
import com.oracle.pic.events.requests.PublishEventsRequest;
import com.oracle.pic.events.responses.PublishEventsResponse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class RecordingEventClient extends EventsIngestionClient {
    private List<BaseEvent> recordedRequests = new LinkedList<>();
    private List<BaseEvent> recordedFailedEvents = new LinkedList<>();
    private Set<String> toFailEvents = new HashSet<>();

    public RecordingEventClient() {
        super(FakeAuthDetailsProvider.Instance);
    }

    @Override
    public PublishEventsResponse publishEvents(PublishEventsRequest request) {
        List<FailedEventRecord> failedEventRecords = new ArrayList<>();
        for (BaseEvent b : request.getEvents().getEvents()) {
            EventV01 e = (EventV01) b;
            if (toFailEvents.contains(e.getEventID())) {
                failedEventRecords.add(new FailedEventRecord("429", "too many request", ((EventV01) b).getEventID()));
                recordedFailedEvents.add(b);
            } else {
                recordedRequests.add(b);
            }
        }
        // toFailEvents filter only applies to one request so that retries work
        toFailEvents.clear();
        return PublishEventsResponse.builder()
            .opcRequestId(request.getOpcRequestId())
                .publishEventsResponseDetails(new PublishEventsResponseDetails(
                        failedEventRecords.size(),
                        failedEventRecords))
                .build();
    }

    public List<BaseEvent> getRecording() {
        return recordedRequests;
    }

    public List<BaseEvent> getFailedEvents() {
        return recordedFailedEvents;
    }

    public void failEventsWithIdsOnNextRequest(Set<String> eventIds) {
        toFailEvents.clear();
        toFailEvents.addAll(eventIds);
    }

    public void clear() {
        recordedRequests.clear();
        recordedFailedEvents.clear();
    }
}
