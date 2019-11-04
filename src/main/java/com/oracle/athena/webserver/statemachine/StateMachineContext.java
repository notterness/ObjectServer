package com.oracle.athena.webserver.statemachine;

/**
 * StateMachineContext - keeps track of where an Object is in the state machine - kept in the executing object.
 */
public class StateMachineContext {
    private int nextState;
    private StateMachine ss = null;

    public StateMachineContext() {
        this.nextState = 0;
    }

    public StateMachineContext(StateMachine ss) {
        nextState = 0;
        this.ss = ss;
    }

    public int getNextState() {
        return nextState;
    }

    public void setNextState(int nextState) {
        this.nextState = nextState;
    }

    public StateMachine getStateMachine() {
        return ss;
    }

    public void setStateMachine(StateMachine ss) {
        this.ss = ss;
    }

}
