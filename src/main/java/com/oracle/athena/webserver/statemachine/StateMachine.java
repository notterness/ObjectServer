package com.oracle.athena.webserver.statemachine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateMachine class: An array of StateEntry objects.
 *
 * @param <K> - any object that flows through a state machine.
 */
public class StateMachine<T,K> {

    private static final Logger LOG = LoggerFactory.getLogger(StateMachine.class);

    private Map<K,StateEntry<T,StateQueueResult> > stateTable = new HashMap<>();

    public void addStateEntry(K verb, StateEntry<T,StateQueueResult> entry) {
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
        StateQueueResult result = null;

        try {
            StateEntry entry = stateTable.get(k);
            result = (StateQueueResult)entry.getVerbExecute().apply(t);
        } catch ( NullPointerException e) {
            LOG.info("no state entry for key " + k);
            e.printStackTrace();
        }

        return result;
    }
}
