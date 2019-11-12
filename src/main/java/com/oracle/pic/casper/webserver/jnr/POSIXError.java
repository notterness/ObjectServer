package com.oracle.pic.casper.webserver.jnr;

import jnr.constants.platform.Errno;

/**
 * An exception that represents errno as wrapped by jnr-constants.
 */
public class POSIXError extends Exception {
    private final Errno errno;

    /**
     * Create an instance from an integer representation of errno.
     * @param errno The errno (e.g. 9 for EBADF)
     * @return The exception
     */
    static POSIXError fromInt(int errno) {
        return new POSIXError(Errno.valueOf(errno));
    }

    /**
     * Wrap an errno object.
     * @param errno The error as an {@link Errno} instance
     */
    POSIXError(Errno errno) {
        super(String.format("%s errno=%d", errno, errno.intValue()));
        this.errno = errno;
    }

    /**
     * Return this exception's underlying errno.
     * @return An {@link Errno} instance
     */
    public Errno getErrno() {
        return errno;
    }
}
