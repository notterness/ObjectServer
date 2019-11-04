package com.oracle.athena.webserver.statemachine;

/**
 * Enum StateQueueResult - indicates what where the object should go after completion of an object state:
 * - STATE_RESULT_FREE - execution is complete
 * - STATE_RESULT_WAIT - blocking call - add to a waitQ
 * - STATE_RESULT_CONTINUE - go to the next state indicated from the executing StateEntry
 * - STATE_RESULT_COMPLETE - the StateMachine is complete, but perhaps not the stack.
 */
public enum StateQueueResult {
    STATE_RESULT_FREE,
    STATE_RESULT_WAIT,
    STATE_RESULT_CONTINUE,
    STATE_RESULT_COMPLETE
}
