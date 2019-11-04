package com.oracle.athena.webserver.statemachine;

import java.util.ArrayList;

/**
 * StateMachine class: An array of StateEntry objects.
 *
 * @param <T> - any object that flows through a state machine.
 */
public class StateMachine<T> {
    private ArrayList<StateEntry> stateList = new ArrayList<>();

    public void addStateEntry(StateEntry entry) {
        stateList.add(entry);
    }

    public StateEntry getState(int stateNum) {
        return stateList.get(stateNum);
    }

    public int getLastState() {
        return this.stateList.size();
    }

    /**
     * Method: stateMachineExecute - runs a StateTable until block or complete
     *
     * @param t              - object that is running the state machine.
     * @param executingState - state to begin execution.
     * @return StateEntryResult - result
     */
    public StateEntryResult stateMachineExecute(T t, int executingState) {
        int stateIndex = executingState;
        int nextIndex;
        int lastIndex = this.getLastState();
        StateEntryResult result;
        StateEntry.StateResultIndex index;

        do {
            StateEntry currentState = stateList.get(stateIndex);

            index = currentState.execute(t);
            result = currentState.getResultEntry(index);
            if (result.getQueueResult() == StateQueueResult.STATE_RESULT_CONTINUE) {
                stateIndex = result.getNextState();
            }

        } while (result.getQueueResult() == StateQueueResult.STATE_RESULT_CONTINUE);

        return result;
    }

    /**
     * Method: stateMachineCallbackComplete - executes the complete function of a blocking state. This state
     * should advance the state executing position.
     *
     * @param t         - objext that contains the state machine
     * @param nextIndex - next execution state.
     * @return
     */
    public StateEntryResult stateMachineCallbackComplete(T t, int nextIndex) {
        StateEntry entry = stateList.get(nextIndex);
        StateEntry.StateResultIndex index;
        StateEntryResult result;

        index = entry.complete(t);
        result = entry.getResultEntry(index);
        return result;
    }
}
