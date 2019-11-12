package com.oracle.pic.casper.webserver.server;

import com.google.common.collect.ImmutableList;
import com.oracle.pic.casper.common.config.v2.EagleConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Determine what addresses the web server can bind on this host by partitioning them into three categories:
 *
 * 1) Public VIPs on an overlay interface (see {@link BindableAddresses#getPublicVIPs()};
 * 2) Private VIPs on an overlay interface (see {@link BindableAddresses#getPrivateVIPs()};
 * 3) Substrate IPs including loopback addresses like 127.0.0.1 (see {@link BindableAddresses#getSubstrateIPs()}.
 *
 * Each of these should have different APIs bound to them (see {@link WebServerVerticle}.
 */
final class BindableAddresses {
    private static final Logger LOG = LoggerFactory.getLogger(BindableAddresses.class);
    /**
     * The brain trust behind Java made NetworkInterface a final class with no public constructors,
     * thus this wrapper interface that allows tests to be written.
     */
    interface NetworkInterface {
        String getDisplayName();
        List<InetAddress> getInetAddresses();
    }

    /**
     * Partition the network interfaces on this host into public, private, and substrate addresses.
     *
     * @param configuration the configuration that contains VIP CIDR ranges and the prefix that identifies
     *                      the overlay interface
     * @return an instance with the partitioned addresses
     */
    static BindableAddresses fromNetworkInterfaces(EagleConfiguration configuration) {
        // Tycho JDT compiler can't infer the type of the stream version of this code.
        final List<java.net.NetworkInterface> physicalInterfaces;
        try {
            physicalInterfaces = Collections.list(java.net.NetworkInterface.getNetworkInterfaces());
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        final List<NetworkInterface> interfaces = new ArrayList<>();
        physicalInterfaces.forEach(i ->
            interfaces.add(new NetworkInterface() {
                @Override
                public String getDisplayName() {
                    return i.getDisplayName();
                }

                @Override
                public List<InetAddress> getInetAddresses() {
                    return Collections.list(i.getInetAddresses());
                }
            })
        );
        return BindableAddresses.fromInterfaces(ImmutableList.copyOf(interfaces), configuration);
    }

    /**
     * Partition a list of network interfaces into public, private, and substrate addresses.  Exposed for testing.
     *
     * @param interfaces the list of interfaces to partitio
     * @param configuration the configuration that contains VIP CIDR ranges and the prefix that identifies
     *                      the overlay interface
     * @return an instance with the partitioned addresses
     */
    static BindableAddresses fromInterfaces(
            List<BindableAddresses.NetworkInterface> interfaces, EagleConfiguration configuration
    ) {
        final List<InetAddress> publicVIPs = new ArrayList<>();
        final List<InetAddress> privateVIPs = new ArrayList<>();
        final List<InetAddress> substrateIPs = new ArrayList<>();

        interfaces.forEach(networkInterface -> {
            final String name = networkInterface.getDisplayName();
            final boolean isOverlay = name.startsWith(configuration.getOverlayInterfacePrefix());
            final List<InetAddress> inets = networkInterface.getInetAddresses();
            inets.forEach(inet -> {
                if (!(inet instanceof Inet4Address)) {
                    LOG.info("Skipping non IPv4 address {}", inet);
                    return;
                }
                if (isOverlay && configuration.isPublicVIP(inet)) {
                    LOG.info("Found public VIP {} on overlay device {}", inet.getHostAddress(), name);
                    publicVIPs.add(inet);
                } else if (isOverlay && configuration.isPrivateVIP(inet)) {
                    LOG.info("Found private VIP {} on overlay device {}", inet.getHostAddress(), name);
                    privateVIPs.add(inet);
                } else if (!isOverlay && !configuration.isPrivateVIP(inet) && !configuration.isPublicVIP(inet)) {
                    LOG.info("Found substrate IP {} on device {}", inet.getHostAddress(), name);
                    substrateIPs.add(inet);
                } else {
                    throw new RuntimeException(
                            String.format(
                                    "interface %s is not an overlay interface but has a VIP (%s)" +
                                    " in the public or private CIDR ranges",
                                    networkInterface.getDisplayName(),
                                    inet.getHostAddress()
                            )
                    );
                }
            });
        });

        return new BindableAddresses(
                ImmutableList.copyOf(publicVIPs),
                ImmutableList.copyOf(privateVIPs),
                ImmutableList.copyOf(substrateIPs)
        );
    }

    private final List<InetAddress> publicVIPs;
    private final List<InetAddress> privateVIPs;
    private final List<InetAddress> substrateIPs;

    private BindableAddresses(
            List<InetAddress> publicVIPs,
            List<InetAddress> privateVIPs,
            List<InetAddress> substrateIPs
    ) {
        this.publicVIPs = publicVIPs;
        this.privateVIPs = privateVIPs;
        this.substrateIPs = substrateIPs;
    }

    /**
     * Return the public VIPs, if any, on this host.
     *
     * @return a list of public VIPs
     */
    List<InetAddress> getPublicVIPs() {
        return publicVIPs;
    }

    /**
     * Return the private VIPs, if any, on this host.
     *
     * @return a list of private VIPs
     */
    List<InetAddress> getPrivateVIPs() {
        return privateVIPs;
    }

    /**
     * Return the substrate IPs on this host.
     *
     * @return a list of substrate IPs
     */
    List<InetAddress> getSubstrateIPs() {
        return substrateIPs;
    }
}
