package com.oracle.athena.webserver.statemachine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * StateMachine class: An array of StateEntry objects.
 *
 * @param <K> - any object that flows through a state machine.
 */
public class StateMachine<T,K> {

    private Map<K,StateEntry> stateTable = new HashMap<>();

    public void addStateEntry(K verb, StateEntry entry) {
        stateTable.put(verb, entry);
    }

    public StateEntry getState(K verb) {
        return stateTable.get(verb);
    }

    /**
     * Method: stateMachineExecute - runs a StateTable until block or complete
     *
     * @param t              - object that is running the state machine.
     * @return StateEntryResult - result
     */
    public StateQueueResult stateMachineExecute( T t, K k) {
        StateQueueResult result = StateQueueResult.STATE_RESULT_CONTINUE;

        try {
            StateEntry state = stateTable.get(k);
            result = state.verbExecutor(t);
        } catch ( NullPointerException e) {
            System.out.println("no state entry for key " + k);
            e.printStackTrace();
        }
        return result;
    }
}
