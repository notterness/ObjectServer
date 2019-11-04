package com.oracle.athena.webserver.statemachine;

/**
 * Class StateEntryResult - indicates the next execution object and queueResult
 */
public class StateEntryResult {
    private int nextState;
    private StateQueueResult queueResult;

    StateEntryResult(int nextState, StateQueueResult queueResult) {
        this.nextState = nextState;
        this.queueResult = queueResult;
    }

    public int getNextState() {
        return nextState;
    }

    public void setNextState(int nextState) {
        this.nextState = nextState;
    }

    public StateQueueResult getQueueResult() {
        return queueResult;
    }

    public void setQueueResult(StateQueueResult queueResult) {
        this.queueResult = queueResult;
    }
}
