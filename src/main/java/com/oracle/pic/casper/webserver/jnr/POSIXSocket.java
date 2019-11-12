package com.oracle.pic.casper.webserver.jnr;

import com.google.common.base.Preconditions;
import jnr.constants.Constant;
import jnr.constants.Platform;
import jnr.constants.platform.AddressFamily;
import jnr.constants.platform.IPProto;
import jnr.constants.platform.SocketLevel;
import jnr.constants.platform.SocketOption;
import jnr.constants.platform.TCP;
import jnr.ffi.LastError;
import jnr.ffi.LibraryLoader;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.byref.IntByReference;

/**
 * Expose unsupported socket operations, like options beyond those covered by {@link SocketOption}, {@link
 * java.net.SocketOption}, or {@link io.netty.channel.ChannelOption}.
 *
 * Supports client {@link java.net.Socket}s and integer file descriptors.
 *
 * <H1>Usage Notes</H1>
 *
 * All functions accept an integer file descriptor.  There are many different ways to represent a socket in Java, and
 * rather than support each one directly, this class assumes callers can derive a file descriptor from their particular
 * socket type.  <b>It is the caller's responsibility to ensure the socket and its parent object are not garbage
 * collected during an invocation!</b>  Long-lived objects, like {@link io.vertx.core.http.HttpServerRequest} instances,
 * should be safe against unexpected garbage collections.
 *
 * {@link POSIXSocket#getsockopt(int, Levels, Options, int)} and {@link POSIXSocket#setsockopt(int, Levels, Options,
 * byte[])} return and expect their options' values as byte arrays. If you want to work with an integer, you have to
 * convert it to and from its binary representation. {@link Octets} provides a convenient way to pass integers as
 * bytes:
 *
 * <pre>
 * POSIXSocket.setsockopt(
 *     fd, POSIXSocket.Levels.SOL_SOCKET,
 *     POSIXSocket.Options.SO_REUSEADDR,
 *     Octets.LITTLE_ENDIAN.toByteArray(1);
 * </pre>
 *
 * And to accept bytes as integers:
 *
 * <pre>
 * bytes = POSIXSocket.getsockopt(
 *     fd, POSIXSocket.Levels.SOL_SOCKET, POSIXSocket.Options.SO_REUSEADDR, Octets.SIZE_OF_INT);
 * int value = Octets.LITTLE_ENDIAN.longFrom(bytes);
 * </pre>
 */

public final class  POSIXSocket {
    private POSIXSocket() {
    }

    /**
     * Socket option levels:
     *
     * <quote>
     * When manipulating socket options, the level at which the option resides and the name of the option must be
     * specified.  To manipulate options at the sockets API level, level is specified as SOL_SOCKET. To manipulate
     * options at any other level the protocol number of the appropriate protocol controlling the option is supplied.
     * For example, to indicate that an option is to be interpreted by the TCP protocol, level should be set to the
     * protocol number of TCP; see <a href="http://man7.org/linux/man-pages/man3/getprotoent.3.html">getprotoent(3).</a>
     * </quote>
     *
     * From <a href="http://man7.org/linux/man-pages/man2/setsockopt.2.html">setsockopt(2)</a>
     */
    public enum Levels {
        /**
         * The IP socket option level.  See <a href="http://man7.org/linux/man-pages/man7/ip.7.html">ip(7)</a>
         */
        IPPROTO_IP(IPProto.IPPROTO_IP),

        /**
         * The TCP socket option level.  See <a href="http://man7.org/linux/man-pages/man7/tcp.7.html">tcp(7)</a>
         */
        IPPROTO_TCP(IPProto.IPPROTO_TCP),

        /**
         * The "socket" socket option level. See <a href="http://man7.org/linux/man-pages/man2/setsockopt.2.html">setsockopt(2)</a>.
         * Currently only used by tests.
         */
        SOL_SOCKET(SocketLevel.SOL_SOCKET);

        private Constant constant;

        Levels(final Constant constant) {
            this.constant = constant;
        }
    }

    /**
     * A socket option undefined in jnr-constants.
     */
    private static final class Option implements Constant {
        private final int value;
        private final String name;

        private Option(String name, int value) {
            this.value = value;
            this.name = name;
        }

        @Override
        public int intValue() {
            return value;
        }

        @Override
        public long longValue() {
            return (long) value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean defined() {
            return true;
        }

        /**
         * A socket {@link Option} representing IP_OPTIONS. See <a href="http://man7.org/linux/man-pages/man7/ip.7.html">ip(7)</a>
         *
         * @return The option for use by
         */
        private static Option ipOptions() {
            int value;
            switch (Platform.OS) {
                case "linux":
                    value = 0x04;
                    break;
                case "darwin":
                    value = 0x01;
                    break;
                default:
                    throw new RuntimeException(String.format("Unknown platform %s", Platform.OS));
            }
            return new Option("IP_OPTIONS", value);
        }
    }

    /**
     * Socket option names.
     *
     * See <a href="http://man7.org/linux/man-pages/man2/setsockopt.2.html">setsockopt(2)</a>
     */
    public enum Options {
        /**
         * The IP datagram options.  Service Gateway communicates the originating VCN, Service Gateway instance, and
         * client IP as a Route Record (RR) option.
         * <pre>
         * IP_OPTIONS (since Linux 2.0)
         *    Set or get the IP options to be sent with every packet from
         *    this socket.  The arguments are a pointer to a memory buffer
         *    containing the options and the option length.  The
         *    setsockopt(2) call sets the IP options associated with a
         *    socket.  The maximum option size for IPv4 is 40 bytes.  See
         *    RFC 791 for the allowed options.  When the initial connection
         *    request packet for a SOCK_STREAM socket contains IP options,
         *    the IP options will be set automatically to the options from
         *    the initial packet with routing headers reversed.  Incoming
         *    packets are not allowed to change options after the connection
         *    is established.  The processing of all incoming source routing
         *    options is disabled by default and can be enabled by using the
         *    accept_source_route /proc interface.  Other options like time‐
         *    stamps are still handled.  For datagram sockets, IP options
         *    can be only set by the local user.  Calling getsockopt(2) with
         *    IP_OPTIONS puts the current IP options used for sending into
         *    the supplied buffer.
         * </pre>
         */
        IP_OPTIONS(Option.ipOptions()),
        /**
         * Allow port reuse after a close.  This is covered by {@link java.net.Socket#getReuseAddress()} and {@link
         * java.net.Socket#setReuseAddress(boolean)}, and is included for tests.
         */
        SO_REUSEADDR(SocketOption.SO_REUSEADDR),

        /**
         * The maximum segment size for outgoing TCP packets.
         * <pre>
         *      The maximum segment size for outgoing TCP packets.  In Linux
         *      2.2 and earlier, and in Linux 2.6.28 and later, if this option
         *      is set before connection establishment, it also changes the
         *      MSS value announced to the other end in the initial packet.
         *      Values greater than the (eventual) interface MTU have no
         *      effect.  TCP will also impose its minimum and maximum bounds
         *      over the value provided.
         * </pre>
         */
        TCP_MAXSEG(TCP.TCP_MAXSEG);

        private Constant constant;

        Options(final Constant constant) {
            this.constant = constant;
        }
    }

    /**
     * A generic SockAddr struct that will work on either macOS or Linux.
     */
    public abstract static class SockAddr extends Struct {
        SockAddr(Runtime runtime) {
            super(runtime);
        }

        /**
         * The socket's family as understood by {@link AddressFamily}
         *
         * @return The integer address family.
         */
        abstract int family();

        /**
         * The size of the struct.
         *
         * @return The integer size.
         */
        int size() {
            return Struct.size(this);
        }
    }

    /**
     * The Linux sockaddr struct.
     */
    public static final class LinuxSockAddr extends SockAddr {
        @SuppressWarnings({"checkstyle:membername", "checkstyle:visibilitymodifier"})
        public final Unsigned16 sa_family = new Unsigned16();

        LinuxSockAddr() {
            super(RUNTIME);
        }

        int family() {
            return sa_family.intValue();
        }
    }

    /**
     * The macOS sockaddr struct.
     */
    public static final class DarwinSockAddr extends SockAddr {
        @SuppressWarnings({"checkstyle:membername", "checkstyle:visibilitymodifier"})
        public final Unsigned8 sa_len = new Unsigned8();
        @SuppressWarnings({"checkstyle:membername", "checkstyle:visibilitymodifier"})
        public final Unsigned8 sa_family = new Unsigned8();

        DarwinSockAddr() {
            super(RUNTIME);
        }

        int family() {
            return sa_family.intValue();
        }
    }

    /**
     * Regarding {@link Lib#getsockopt(int, int, int, byte[], IntByReference)} and {@link Lib#setsockopt(int, int, int,
     * byte[], int)}:
     *
     * <pre>
     * The arguments optval and optlen are used to access option values for
     * setsockopt().  For getsockopt() they identify a buffer in which the
     * value for the requested option(s) are to be returned.  For
     * getsockopt(), optlen is a value-result argument, initially containing
     * the size of the buffer pointed to by optval, and modified on return
     * to indicate the actual size of the value returned.  If no option
     * value is to be supplied or returned, optval may be NULL.
     * </pre>
     */
    public interface Lib {

        /**
         * http://man7.org/linux/man-pages/man2/getsockopt.2.html
         *
         * @param sockfd  See the man page
         * @param level   See the man page
         * @param optname See the man page
         * @param optval  See the man page
         * @param optlen  See the man page
         * @return On success, zero is returned.  On error, -1 is returned, and errno is set appropriately
         */
        int getsockopt(int sockfd, int level, int optname, byte[] optval, IntByReference optlen);

        /**
         * http://man7.org/linux/man-pages/man2/setsockopt.2.html
         *
         * @param sockfd  See the man page
         * @param level   See the man page
         * @param optname See the man page
         * @param optval  See the man page
         * @param optlen  See the man page
         * @return On success, zero is returned.  On error, -1 is returned, and errno is set appropriately
         */
        int setsockopt(int sockfd, int level, int optname, byte[] optval, int optlen);

        /**
         * http://man7.org/linux/man-pages/man2/getsockname.2.html
         *
         * @param sockfd  See the man page
         * @param addr    See the man page
         * @param addrlen See the man page
         * @return On success, zero is returned.  On error, -1 is returned, and errno is set appropriately
         */
        int getsockname(int sockfd, SockAddr addr, IntByReference addrlen);
    }

    private static final Lib LIB = LibraryLoader.create(Lib.class).load("c");
    private static final Runtime RUNTIME = Runtime.getRuntime(LIB);

    /**
     * Get the socket option at the given level from the socket represented by the file descriptor.
     *
     * @param fd                  A file descriptor representing a socket
     * @param level               The option level
     * @param option              The option
     * @param maximumOptionLength The maximum size the returned option can occupy
     * @return The option's value as a byte array.
     * @throws POSIXError See the man page for getsockopt(2)
     */
    public static byte[] getsockopt(int fd, Levels level, Options option, int maximumOptionLength) throws POSIXError {
        Preconditions.checkArgument(option.constant.defined());

        byte[] optval = new byte[maximumOptionLength];
        IntByReference optlen = new IntByReference(maximumOptionLength);

        int result = LIB.getsockopt(fd, level.constant.intValue(), option.constant.intValue(), optval, optlen);
        if (result == -1) {
            throw POSIXError.fromInt(LastError.getLastError(RUNTIME));
        }

        int returnLength = optlen.getValue();
        if (returnLength == optval.length) {
            return optval;
        } else {
            byte[] data = new byte[returnLength];
            System.arraycopy(optval, 0, data, 0, returnLength);
            return data;
        }
    }

    /**
     * Set the socket option at the given level on the socket represented by the file descriptor.
     *
     * @param fd     A file descriptor representing a socket.
     * @param level  The option level
     * @param option The option
     * @param optval The option's value as a byte array.
     * @throws POSIXError See the man page for setsockopt(2)
     */
    public static void setsockopt(int fd, Levels level, Options option, byte[] optval) throws POSIXError {
        Preconditions.checkArgument(option.constant.defined());

        int result = LIB.setsockopt(fd, level.constant.intValue(), option.constant.intValue(), optval, optval.length);
        if (result == -1) {
            throw POSIXError.fromInt(LastError.getLastError(RUNTIME));
        }
    }


    /**
     * A socket's family.
     */
    public enum Family {
        IPv4(),
        IPv6(),
        UNKNOWN()
    }

    /**
     * Utility function that returns a socket's family.  Java 8's "dual stack" support purposely obscures a bound or
     * connected socket's family, but we need to know it to access family specific options like IP_OPTIONS.
     *
     * @param fd A file descriptor that represents the socket
     * @return A {@link Family} instance describing the socket's family.
     * @throws POSIXError See the man page for gethostname(2)
     */
    public static Family family(int fd) throws POSIXError {
        SockAddr addr;
        switch (Platform.OS) {
            case "linux":
                addr = new LinuxSockAddr();
                break;
            case "darwin":
                addr = new DarwinSockAddr();
                break;
            default:
                throw new RuntimeException(String.format("Unknown platform %s", Platform.OS));
        }
        IntByReference addrlen = new IntByReference(Struct.size(addr));
        int result = LIB.getsockname(fd, addr, addrlen);
        if (result == -1) {
            throw POSIXError.fromInt(LastError.getLastError(RUNTIME));
        }

        if (addrlen.getValue() < addr.size()) {
            throw new RuntimeException("Unexpected SockAddr size");
        }

        int family = addr.family();
        if (family == AddressFamily.AF_INET.intValue()) {
            return Family.IPv4;
        } else if (family == AddressFamily.AF_INET6.intValue()) {
            return Family.IPv6;
        } else {
            return Family.UNKNOWN;
        }
    }
}
