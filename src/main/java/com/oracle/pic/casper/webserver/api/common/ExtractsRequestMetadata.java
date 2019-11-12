package com.oracle.pic.casper.webserver.api.common;

import com.google.common.net.InetAddresses;
import com.oracle.pic.casper.common.rest.CommonHeaders;
import com.oracle.pic.casper.webserver.jnr.Octets;
import com.oracle.pic.casper.webserver.jnr.POSIXError;
import com.oracle.pic.casper.webserver.jnr.POSIXSocket;
import io.netty.channel.Channel;
import io.netty.channel.nio.AbstractNioChannel;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.impl.ConnectionBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.SelChImpl;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Optional;

/**
 * Extract request metadata.
 */
class ExtractsRequestMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractsRequestMetadata.class);
    /**
     * The port on which the Eagle server is listening.
     */
    private final int eaglePort;

     /**
     * The Route Record option should be 20 bytes long
     *
     * See <a href="https://tools.ietf.org/html/rfc791">RFC 791</a> for a description of the option.
     *
     * See <a href="https://confluence.oci.oraclecorp.com/display/CASPER/Eagle+Research#EagleResearch-ServiceGatewaytoObjectStorage">the
     * wiki</a> for an explanation of how Service Gateway uses it.
     */
    private static final int SGW_OPTION_LENGTH = 20;
    /**
     * Skip the first three bytes that represent the Route Record header: type, length, and pointer.
     */
    private static final int SGW_METADATA_OFFSET = 3;

    /**
     * The VCN ID is a 64 bit integer.
     */
    private static final int SGW_METADATA_LEN = 8;

    /**
     * Skip the first three bytes (metadata offset) & the metadata itself which is another 8 bytes.
     */
    private static final int SGW_SOURCE_ADDR_OFFSET = SGW_METADATA_OFFSET + SGW_METADATA_LEN;

    /**
     * The source address is a 32bit integer
     */
    private static final int SGW_SOURCE_ADDR_LEN = 4;


    ExtractsRequestMetadata(int eaglePort) {
        this.eaglePort = eaglePort;
    }

    boolean isEagle(HttpServerRequest request) {
        return request.localAddress().port() == eaglePort;
    }

    /**
     * true/false if the current request originated from ServiceGateway
     * Any exception trying to read from the socket returns false.
     */
    boolean isSGW(HttpServerRequest request) {
        try {
            return vcnIDFromRequest(request) != null;
        } catch (POSIXError posixError) {
            return false;
        }
    }

    /**
     * Extract the integer file descriptor representing this request's socket.
     *
     * @param request A {@link HttpServerRequest}
     * @return The underlying file descriptor
     */
    private static int fdFromRequest(final HttpServerRequest request) {
        final HttpConnection connection = request.connection();
        final ConnectionBase connectionBase = (ConnectionBase) connection;
        final Channel nettyChannel = connectionBase.channel();
        final AbstractNioChannel.NioUnsafe unsafeWithChannel = (AbstractNioChannel.NioUnsafe) (nettyChannel.unsafe());
        final SocketChannel channel = (SocketChannel) unsafeWithChannel.ch();
        return ((SelChImpl) channel).getFDVal();
    }

    /**
     * Extract the VCN ID from an IP Route Record Option.
     *
     * @param optval The IP options (see {@link POSIXSocket#getsockopt(int, POSIXSocket.Levels, POSIXSocket.Options,
     *               int)}
     * @return The VCN ID as a string if it's available and null otherwise.  NB: the VCN ID is an unsigned 64 bit
     * integer, but Java longs are signed, so it should be kept as a string!
     */
    @Nullable
    static String vcnIDFromIPOptions(byte[] optval) {
        if (optval.length < SGW_OPTION_LENGTH) {
            return null;
        }
        ByteBuffer scratch = ByteBuffer.allocate(optval.length);

        scratch.put(optval, SGW_METADATA_OFFSET, SGW_METADATA_LEN);
        scratch.flip();
        scratch.order(ByteOrder.BIG_ENDIAN);
        return Long.toUnsignedString(scratch.getLong());
    }

    /**
     * Extract the ServiceGateway source address from an IP Route Record Option.
     *
     * @param optval The IP options (see {@link POSIXSocket#getsockopt(int, POSIXSocket.Levels, POSIXSocket.Options,
     *               int)}
     * @return The source address as a string if it's available and null otherwise.
     */
    @Nullable
    static String sourceAddressFromIPOptions(byte[] optval) {
        if (optval.length < SGW_OPTION_LENGTH) {
            return null;
        }
        ByteBuffer scratch = ByteBuffer.allocate(optval.length);

        scratch.put(optval, SGW_SOURCE_ADDR_OFFSET, SGW_SOURCE_ADDR_LEN);
        scratch.flip();
        scratch.order(ByteOrder.BIG_ENDIAN);
        return InetAddresses.fromInteger(scratch.getInt()).getHostAddress();
    }

    /**
     * Fetch the IP options from an {@link HttpServerRequest}
     * The caller should take care to check that the returned array
     * has at least {@link #SGW_OPTION_LENGTH} otherwise consider that the
     * request <b>did not</b> have SericeGateway IP options.
     */
    static byte[] ipOptions(HttpServerRequest request) throws POSIXError {
        int fd = fdFromRequest(request);
        if (!POSIXSocket.family(fd).equals(POSIXSocket.Family.IPv4)) {
            throw new RuntimeException("IPv4 required");
        }
        return POSIXSocket.getsockopt(
                fd,
                POSIXSocket.Levels.IPPROTO_IP,
                POSIXSocket.Options.IP_OPTIONS,
                SGW_OPTION_LENGTH);
    }

    /**
     * Return the VCN ID, if any, in the request.  If this is an Eagle request, the VCN ID is extracted from the socket.
     * Otherwise, it's extracted from the X-Vcn-Id header.
     *
     * NB: This has a potential side effect: it removes X-Vcn-Id header from Eagle requests to ensure
     * they cannot affect other code.*
     *
     * @param request The client's request
     * @return The VCN ID or null if none was provided.
     */
    @Nullable
    String vcnIDFromRequest(HttpServerRequest request) throws POSIXError {
        if (isEagle(request)) {
            // A valid Eagle request cannot include X-Vcn-Id.  Mimic the Flamingos' behavior by stripping it out now.
            request.headers().remove(CommonHeaders.X_VCN_ID);
            LOG.debug("Eagle IPv4 request - looking for VCN ID in IP options");
            byte[] ipOptions = ipOptions(request);
            return vcnIDFromIPOptions(ipOptions);
        } else {
            return request.getHeader(CommonHeaders.X_VCN_ID);
        }
    }

    /**
     * Return the remote host.  If this is an Eagle request, that's the socket's remote host.  Otherwise, a Flamingo
     * might have provided it in X-Real-Ip.
     *
     * NB: This has a potential side effect: it removes X-Real-Ip header from Eagle requests to ensure
     * they cannot affect other code.
     *
     * @param request The client's request
     * @return The remote host.  Falls back to the remote socket address.
     */
    @NotNull
    String clientIP(HttpServerRequest request) throws POSIXError {
        String remoteHost = request.remoteAddress().host();
        if (isEagle(request)) {
            // A valid Eagle request cannot include X-Real-Ip. Mimic the Flamingos' behavior by stripping it out now.
            request.headers().remove(CommonHeaders.X_REAL_IP);

            // if it is a ServiceGateway request, the X_REAL_IP should be the value in the IP_OPTIONS
            // and not the remote address
            if (isSGW(request)) {
                LOG.debug("Eagle IPv4 request - looking for source address in IP options");
                byte[] ipOptions = ipOptions(request);
                remoteHost = sourceAddressFromIPOptions(ipOptions);
            }

            LOG.debug("Eagle request - client IP will be remote IP {}", remoteHost);
            return remoteHost;
        } else {
            return Optional.ofNullable(request.getHeader(CommonHeaders.X_REAL_IP))
                    .orElse(remoteHost);
        }
    }

    /**
     * Return the server host the request was aimed for. If this is an eagle request, then this is the origin server.
     * Otherwise, a Flamingo (LB) will provide the original host through this header
     *
     * @param request The client's request
     * @return the server host
     */
    @NotNull
    String host(HttpServerRequest request) {
        final String serverHost = request.host();
        if (isEagle(request)) {
            return serverHost;
        } else return Optional.ofNullable(request.getHeader(CommonHeaders.X_FORWARDED_HOST)).orElse(serverHost);
    }

    /**
     * Returns the MSS (Maximum Segmentation Size) for the TCP stream.
     *
     * @param request The client's request
     * @return The mss negotiated between the client & server
     */
    int mss(HttpServerRequest request) {
        try {
            int fd = fdFromRequest(request);
            if (!POSIXSocket.family(fd).equals(POSIXSocket.Family.IPv4)) {
                throw new RuntimeException("IPv4 required");
            }
            LOG.debug("Eagle IPv4 request - looking for VCN ID in IP options");

            byte[] mss = POSIXSocket.getsockopt(
                    fd,
                    POSIXSocket.Levels.IPPROTO_TCP,
                    POSIXSocket.Options.TCP_MAXSEG,
                    Integer.BYTES);
            return Octets.LITTLE_ENDIAN.intFrom(mss);
        } catch (POSIXError e) {
            LOG.debug("could not determine mss", e);
            return 0;
        }
    }
}
