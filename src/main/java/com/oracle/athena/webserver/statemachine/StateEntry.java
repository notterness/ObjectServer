package com.oracle.athena.webserver.statemachine;

import java.util.ArrayList;

/**
 * StateEntry - abstract class that describes the execution of this object - where it goes next, etc
 *
 * @param <T> - Object that is executing the state.
 */
public abstract class StateEntry<T> {
    private int entryNum;
    private boolean blocks = false;
    private ArrayList<StateEntryResult> results;
    private int numResults;

    public enum StateResultIndex {
        STATE_RESULT_INDEX_OK(0),
        STATE_RESULT_INDEX_ERROR(1),
        STATE_RESULT_INDEX_WAIT(2),
        STATE_RESULT_INDEX_MAX(3);

        private final int value;

        StateResultIndex(int value) {
            this.value = value;
        }

        public int toInt() {
            return this.value;
        }
    }

    public StateEntry(int entryNum) {
        StateResultIndex resultMax = StateResultIndex.STATE_RESULT_INDEX_MAX;

        this.entryNum = entryNum;
        this.numResults = 0;
        this.results = new ArrayList<>(resultMax.toInt());
    }

    public abstract StateResultIndex execute(T t);

    public abstract StateResultIndex complete(T t);

    public boolean blocks() {
        return this.blocks;
    }

    public void setBlocks(boolean blocks) {
        this.blocks = blocks;
    }

    public int getEntryNum() {
        return this.entryNum;
    }

    public void addResultEntry(StateEntryResult resultEntry, StateResultIndex index) {
        try {
            results.add(index.toInt(), resultEntry);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("index greater than capacity: index " + index);
            e.printStackTrace();
        }
        numResults++;
    }

    public StateEntryResult getResultEntry(StateResultIndex index) {
        StateEntryResult result = null;
        try {
            result = results.get(index.toInt());
        } catch (IndexOutOfBoundsException e) {
            System.out.println("index greater than capacity: index " + index);
            e.printStackTrace();
        }
        return result;
    }
}
