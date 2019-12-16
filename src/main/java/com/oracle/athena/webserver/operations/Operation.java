package com.oracle.athena.webserver.operations;

public interface Operation {
    void initialize();

    void eventHandler();

    void execute();

    void complete();
}
