package com.oracle.athena.webserver.statemachine;


/**
 * StateEntry - abstract class that describes the execution of this object - where it goes next, etc
 *
 * @param <T> - Object that is executing the state.
 *
 */
public abstract class StateEntry<T,R> {

    public abstract StateQueueResult verbExecutor(T t);
};
