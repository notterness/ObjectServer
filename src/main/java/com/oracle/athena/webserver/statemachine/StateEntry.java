package com.oracle.athena.webserver.statemachine;


import java.util.function.Function;

/**
 * StateEntry - abstract class that describes the execution of this object - where it goes next, etc
 *
 * @param <T> - Object that is executing the state.
 *
 */
public class StateEntry<T,StateQueueResult> {

    private Function<T,StateQueueResult> verbExecute;

    public StateEntry(Function verbExecute) {
        this.verbExecute = verbExecute;
    }

    public Function<T, StateQueueResult> getVerbExecute() {
        return verbExecute;
    }

    public void setVerbExecute(Function<T, StateQueueResult> verbExecute) {
        this.verbExecute = verbExecute;
    }
}
