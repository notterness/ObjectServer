package com.oracle.pic.casper.webserver.traffic;

import javax.measure.quantity.DataRate;
import javax.measure.unit.NonSI;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;

/**
 * {@link Unit<DataRate>}s for use in measuring requests' bandwidths.
 */
public final class Bandwidths {
    public static final Unit<DataRate> MEGABITS_PER_SECOND = SI.MEGA(SI.BIT)
            .divide(SI.SECOND)
            .asType(DataRate.class);
    public static final Unit<DataRate> MEGABYTES_PER_SECOND = SI.MEGA(NonSI.BYTE)
            .divide(SI.SECOND)
            .asType(DataRate.class);
    public static final Unit<DataRate> BYTES_PER_SECOND = NonSI.BYTE
            .divide(SI.SECOND)
            .asType(DataRate.class);
    public static final Unit<DataRate> BYTES_PER_NANOSECOND = NonSI.BYTE
            .divide(SI.NANO(SI.SECOND))
            .asType(DataRate.class);

    private Bandwidths() {

    }
}
