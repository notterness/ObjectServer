package com.oracle.athena.webserver.connectionstate;

/*
** This is used to check that there has been activity on a Connection within a regular time frame.
**   If the watchdog is not updated on a regular basis, it will return an error and it is up to the
**   owner to close out the connection channel properly.
 */

import java.sql.Timestamp;

public class TimeoutChecker {

    /*
    ** Currently set the inactivity time to 5 seconds
     */
    public static final long CHANNEL_INACTIVITY_TIME = (1000 * 5);

    private long lastUpdateTimeMs;
    private long worstCaseTimeDelta;

    TimeoutChecker() {
        lastUpdateTimeMs = System.currentTimeMillis();

        worstCaseTimeDelta = 0;
    }

    /*
    ** This function should be called whenever there is activity on a channel to indicate that
    **   progress is being made and the client is actually being responsive in terms of sending
    **   or receiving data.
    ** This returns true if the delta is greater than the allowable time between operations on the
    **   channel.
     */
    boolean updateTime() {
        boolean exceededTime = false;

        long currTime = System.currentTimeMillis();

        long delta = (currTime - lastUpdateTimeMs);
        if (delta > worstCaseTimeDelta) {
            worstCaseTimeDelta = delta;
        }

        if (delta > CHANNEL_INACTIVITY_TIME)
        {
            exceededTime = true;
        }
        lastUpdateTimeMs = currTime;

        return exceededTime;
    }

    /*
    ** If updateTime() has not been called within the CHANNEL_INACTIVITY_TIME this will return
    **   true. It is up to the caller to then close out the channel and terminate the
    **   connection.
     */
    boolean inactivityThresholdReached() {
        boolean exceededTime = false;

        long currTime = System.currentTimeMillis();

        long delta = (currTime - lastUpdateTimeMs);
        if (delta > CHANNEL_INACTIVITY_TIME)
        {
            Timestamp lastUpdate = new Timestamp(lastUpdateTimeMs);
            Timestamp curr = new Timestamp(currTime);

            System.out.println("TimeoutChecker - exceeded timeout: lastUpdate - " + lastUpdate.toString() +
                    " current - " + curr.toString());
            exceededTime = true;
        }

        return exceededTime;
    }
}
