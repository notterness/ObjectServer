package com.oracle.athena.webserver.statemachine;

import java.util.ArrayDeque;

/**
 * Class StateMachineStack implements a stack of StateMachines
 */
public class StateMachineStack {
    private ArrayDeque<StateMachineContext> stateMachines = new ArrayDeque<>();

    public StateMachineStack(StateMachine ss) {
        StateMachineContext ssContext;

        ssContext = new StateMachineContext(ss);
        stateMachines.add(ssContext);
    }

    public void setNextState(int nextState) {
        StateMachineContext elem = stateMachines.getLast();
        elem.setNextState(nextState);
    }

    public int getNextState() {
        StateMachineContext elem = stateMachines.getLast();

        return elem.getNextState();
    }

    public StateEntry getNextEntry() {
        StateMachineContext elem = stateMachines.getLast();
        int nextState = elem.getNextState();
        StateMachine ss = elem.getStateMachine();

        return ss.getState(elem.getNextState());

    }

    public StateMachineContext getStateMachineContext() {
        return stateMachines.getLast();
    }

    public StateMachine getStateMachine() {
        StateMachineContext elem = stateMachines.getLast();
        return elem.getStateMachine();
    }

    public void pushStateMachine(StateMachine ss) {
        StateMachineContext elem = new StateMachineContext(ss);
        this.stateMachines.push(elem);
    }

    public void popStateMachine() {
        if (stateMachines.size() > 1) {
            this.stateMachines.pop();
        }
    }

    public int getStackDepth() {
        return stateMachines.size();
    }
}
