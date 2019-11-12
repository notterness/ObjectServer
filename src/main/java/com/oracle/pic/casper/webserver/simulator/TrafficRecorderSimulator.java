package com.oracle.pic.casper.webserver.simulator;

import com.oracle.pic.casper.webserver.traffic.Bandwidths;
import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.measure.Measure;
import javax.measure.unit.SI;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Scanner;

/**
 * Simulator to log data on how well the traffic recorder estimates bandwidth in different scenarios.
 */

public class TrafficRecorderSimulator {

    static final long SECOND = 1_000_000_000;
    static final long MILLISECOND = 1_000_000;
    static final String TENANT_NAME_SPACE = "TRAFFIC_RECORDER";

    /**
     * Queue of events to be processed by the server and java. Ordered first by time and then by the order of events in
     * the original traffic log (this is to preserve order after simulated garbage collection pause causes bunching).
     */
    private PriorityQueue<TrafficEvent> priorityQueue;

    /**
     * Traffic Recorder that the simulation will call methods on to see its bandwidth estimates.
     */
    private final TrafficRecorder trafficRecorder;

    public TrafficRecorderSimulator() {
        priorityQueue = new PriorityQueue<>(Comparator.comparingLong(TrafficEvent::getTime)
                .thenComparingLong(TrafficEvent::getOriginalIndex));

        TrafficRecorder.Configuration config = new TrafficRecorder.Configuration(
                Measure.valueOf(32_000_000, Bandwidths.BYTES_PER_SECOND),
                Measure.valueOf(50, SI.MILLI(SI.SECOND)),
                .5,
                .25,
                Measure.valueOf(1, SI.MILLI(SI.SECOND)),
                2
        );
        trafficRecorder = new TrafficRecorder(config);
    }

    /**
     * Simulates a request that is constant throughput for some amount of updates and then stalls. Runs until the error
     * between actual bandwidth (0) and estimated bandwidth (post-stall) is less than .1% of the original Bps. Outputs
     * only this final timestamp, actual, and estimate to the log. Used to see how long it will take for TrafficRecorder
     * to converge to 0 during a stall if before noise or garbage collection pauses are added. In this case the recorder
     * is updated each millisecond after one buffer arrival each.
     *
     * @param bytesPerSecond      number of bytes per second that will be simulated in request
     * @param contentLength       total length in bytes of the request
     * @param requestsBeforeStall number of requests to send at the constant rate before the stall where buffers stop
     *                            arriving
     */
    public void stallConverge(long bytesPerSecond, long contentLength, int requestsBeforeStall) {
        long time = 0;
        String id = "0";
        trafficRecorder.requestStarted(TENANT_NAME_SPACE, id, TrafficRecorder.RequestType.PutObject,
                contentLength, time);
        // .1% of ideal rate
        final long error = bytesPerSecond / 1000;
        final long bytesPerMillisecond = bytesPerSecond / 1000;

        for (int i = 0; i < requestsBeforeStall; i++) {
            time += MILLISECOND;
            trafficRecorder.bufferArrived(id, bytesPerMillisecond, time);
            trafficRecorder.update(time);
        }

        long estimate = trafficRecorder.getBandwidthUsage();
        while (estimate >= error) {
            time += MILLISECOND;
            trafficRecorder.update(time);
            estimate = trafficRecorder.getBandwidthUsage();
        }
        printFormat(time, 0L, estimate);
        trafficRecorder.requestEnded(id, time);
        System.out.println("\n");
    }

    /**
     * Prints to standard output important data in json format as the time in seconds, actual rate in Bps, and the
     * estimated rate in Bps.
     *
     * @param time           current time in nanoseconds
     * @param bytesPerSecond actual rate in bytes per second
     * @param estimate       estimated rate in bytes per second
     */
    private void printFormat(long time, long bytesPerSecond, long estimate) {
        System.out.println(String.format("{\"time\":%.2f,\"actual\":%d,\"estimate\":%d}%n", time / Math.pow(10, 9),
                bytesPerSecond, estimate));
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    /**
     * Takes all events in a given valid traffic log and adds them to the priorityQueue to be simulated with additional
     * java and server noise.
     *
     * @param inputFilename filepath where the traffic log exists
     * @throws FileNotFoundException if the input file cannot be found
     */
    private void addLogToQueue(String inputFilename) throws FileNotFoundException {
        final Scanner scanner = new Scanner(new File(inputFilename), "UTF-8");
        final HashMap<String, Long> totalLengths = new HashMap<>();
        final HashMap<String, Long> totalBytesReceiveds = new HashMap<>();
        final HashMap<String, TrafficRecorder.RequestType> reqTypes = new HashMap<>();
        final HashMap<String, Long> lastTimes = new HashMap<>();

        long originalIndex = 0L;
        while (scanner.hasNextLine()) {
            final String[] lineTokens = scanner.nextLine().split(" ");
            final String type = lineTokens[0];
            final String id = lineTokens[1];
            final long time;
            final Long totalLength;
            final double actualRate;
            final long deltaBytes;
            switch (type) {
                case "start":
                    final TrafficRecorder.RequestType reqType = lineTokens[2].equals("PutObject") ?
                            TrafficRecorder.RequestType.PutObject : TrafficRecorder.RequestType.GetObject;
                    time = Long.parseLong(lineTokens[3]);
                    totalLength = Long.parseLong(lineTokens[4]);
                    if (totalLengths.containsKey(id)) {
                        throw new RuntimeException("Request id was not unique and was started twice");
                    }
                    totalLengths.put(id, totalLength);
                    totalBytesReceiveds.put(id, 0L);
                    priorityQueue.add(new TrafficEvent(TrafficEventType.START_SERVER, id, time, 0L, 0L, 0, totalLength,
                            reqType, originalIndex));
                    lastTimes.put(id, time);
                    break;
                case "update":
                    final long currentBytes = Long.parseLong(lineTokens[2]);
                    time = Long.parseLong(lineTokens[3]);
                    final long totalBytesReceived = totalBytesReceiveds.get(id);
                    if (!totalBytesReceiveds.containsKey(id)) {
                        throw new RuntimeException("Request id was updated before it was started in log");
                    }
                    if (totalLengths.get(id).equals(totalBytesReceived)) {
                        throw new RuntimeException("Request id was updated after all bytes had been sent");
                    }
                    deltaBytes = currentBytes - totalBytesReceived;
                    actualRate = calculateRate(deltaBytes, time, lastTimes.get(id));
                    priorityQueue.add(new TrafficEvent(TrafficEventType.UPDATE_SERVER, id, time, currentBytes,
                            deltaBytes, actualRate, totalLengths.get(id), reqTypes.get(id), originalIndex));
                    totalBytesReceiveds.put(id, currentBytes);
                    lastTimes.put(id, time);
                    break;
                case "end":
                    totalLength = totalLengths.get(id);
                    if (!totalBytesReceiveds.get(id).equals(totalLength)) {
                        throw new RuntimeException("Request id was ended before all bytes had been sent");
                    }
                    time = Long.parseLong(lineTokens[2]);
                    priorityQueue.add(new TrafficEvent(TrafficEventType.END_SERVER, id, time, totalLength, 0L, 0L,
                            totalLength, reqTypes.get(id), originalIndex));
                    break;
                default:
                    break;
            }
            originalIndex++;
        }
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_OUT")
    /**
     * Simulates the events in the log given by the inputFilename and writes traffic controller update events to the
     * given outputFilename. If printAll is true, outputs all other events to stdout either as traditional traffic log
     * updates (per request) (labeled as start, update, end, whereas totalUpdate is all requests) or json line syntax
     * for analysis. Traffic log must be valid (start called before update/end for each id, end called after update
     * indicates that contentLength has been reached without any other updates called).
     *
     * Adds noise delays between event arriving at the "server" (based on the log timestamps) and arriving at "java".
     * These delays are based on a normal distribution with an average of .5 seconds (500000000 nanoseconds) and
     * standard deviation of .1 seconds (100000000 nanoseconds).
     *
     * In the output file, the actual rate corresponds to the rate arriving at the "server". The estimated rate
     * corresponds to the estimated rate outputted by the Traffic Recorder, which only gets buffer arrivals upon arrival
     * at "java". It averages and updates its calculations each millisecond. At each update timestamp, the file reflects
     * the actual and estimated rate over the last update interval.
     *
     * @param inputFilename  file path to log. Log must initiate all requests with a single start and not send any more
     *                       updates after calling end. End calls should only be called after the most recent update's
     *                       bytes received is the same as total content length. No updates should be called after all
     *                       bytes have been sent.
     * @param outputFilename file path to location to write simulated log
     * @param json           true if output should be formatted as single lines of valid json, false if it should be
     *                       formatted to match a traffic log
     * @param printAll       true if log should include all start/end/updates from the input log, false if just traffic
     *                       controller updates
     * @throws FileNotFoundException if if inputFilename or outputFilename filepath is invalid
     */
    public void simulateLogQueue(String inputFilename, String outputFilename, boolean json, boolean printAll)
            throws IOException {
        // over what time period the "actual" rate is averaged (i.e., the averaging window)
        final long ACTUAL_RATE_AVERAGE_WINDOW = MILLISECOND;
        // how often the "actual" rate is _updated_
        final long UPDATE_INTERVAL = MILLISECOND;
        final OutputStreamWriter writer =
                new OutputStreamWriter(new FileOutputStream(outputFilename),
                        StandardCharsets.UTF_8);
        final SecureRandom random = new SecureRandom();
        HashMap<String, Double> lastRateEstimates = new HashMap<>();
        HashMap<Long, Long> bytesByTimestamp = new HashMap<>();
        bytesByTimestamp.put(0L, 0L);
        long updateTime = 0L;
        long lastTimeSent = 0L;
        double estimatedTotalRate = 0;
        double actualTotalRate;
        long time = 0;

        addLogToQueue(inputFilename);

        while (!priorityQueue.isEmpty()) {
            final TrafficEvent event = priorityQueue.poll();
            time = event.getTime();

            while (time > updateTime) {
                // min included in case update of interval is smaller than calculation window
                actualTotalRate = totalRateFromByteBuckets(bytesByTimestamp, Math.min(updateTime,
                        ACTUAL_RATE_AVERAGE_WINDOW));
                if (json) {
                    writer.write(formatOutputJson(updateTime, estimatedTotalRate, actualTotalRate));
                }
                trafficRecorder.update(updateTime);
                estimatedTotalRate = trafficRecorder.getBandwidthUsage();
                updateTime += UPDATE_INTERVAL;
                bytesByTimestamp.put(updateTime, 0L);
                if (updateTime - ACTUAL_RATE_AVERAGE_WINDOW >= 0) {
                    bytesByTimestamp.remove(updateTime - ACTUAL_RATE_AVERAGE_WINDOW);
                }
            }

            final TrafficRecorder.RequestType reqType = event.getReqType();
            final String id = event.getId();
            final long totalBytesReceived = event.getTotalBytesReceived();
            final long deltaBytes = event.getDeltaBytes();
            final long contentLength = event.getContentLength();
            final double actualRate = event.getActualRate();
            final long originalIndex = event.getOriginalIndex();

            // normal distribution: random.nextGaussian() * standardDeviation + average
            final long noiseDelay = (long) (random.nextGaussian() * 0.0 + 0.0);
            // in case last delay was long, this will bunch some arrivals together
            final long newTime = Math.max(time + noiseDelay, lastTimeSent);
            final long timestampBytes = bytesByTimestamp.get(updateTime);
            switch (event.getType()) {
                case START_SERVER:
                    priorityQueue.add(new TrafficEvent(TrafficEventType.START_JAVA, id, newTime, totalBytesReceived,
                            deltaBytes, actualRate, contentLength, reqType, originalIndex));
                    lastTimeSent = newTime;
                    break;
                case UPDATE_SERVER:
                    priorityQueue
                            .add(new TrafficEvent(TrafficEventType.UPDATE_JAVA, id, newTime, totalBytesReceived,
                                    deltaBytes, actualRate, contentLength, reqType, originalIndex));
                    lastTimeSent = newTime;
                    bytesByTimestamp.put(updateTime, timestampBytes + deltaBytes);
                    break;
                case END_SERVER:
                    priorityQueue.add(new TrafficEvent(TrafficEventType.END_JAVA, id, newTime, totalBytesReceived,
                            deltaBytes, actualRate, contentLength, reqType, originalIndex));
                    lastTimeSent = newTime;
                    break;

                case START_JAVA:
                    trafficRecorder.requestStarted(TENANT_NAME_SPACE, id, reqType, contentLength, time);
                    if (printAll) {
                        writer.write(json ? event.toStringJson(0L) : event.toString());
                    }
                    lastRateEstimates.put(id, trafficRecorder.getReqBandwidthUsage(id));
                    break;
                case UPDATE_JAVA:
                    trafficRecorder.bufferArrived(id, deltaBytes);
                    if (printAll) {
                        writer.write(json ? event.toStringJson(lastRateEstimates.get(id)) : event.toString());
                    }
                    lastRateEstimates.put(id, trafficRecorder.getReqBandwidthUsage(id));
                    break;
                case END_JAVA:
                    trafficRecorder.requestEnded(id, time);
                    if (printAll) {
                        writer.write(json ? event.toStringJson(lastRateEstimates.get(id)) : event.toString());
                    }
                    break;
                default:
                    break;
            }
        }
        // technically ignores last window
        writer.close();

    }

    /**
     * Sums up bytes in all values in mapping and calculates total rate over timestep.
     *
     * @param bytesByTimestamp mapping from update timestamps to the total number of bytes received between that update
     *                         and the last
     * @param timestep         timestep duration between updates in nanoseconds
     * @return total rate over timestep in bytes per second
     */
    private double totalRateFromByteBuckets(HashMap<Long, Long> bytesByTimestamp, long timestep) {
        long bytesSinceLastUpdate = bytesByTimestamp.values().stream().reduce(0L, (a, b) -> a + b);
        return Measure.valueOf((bytesSinceLastUpdate / (double) timestep), Bandwidths.BYTES_PER_NANOSECOND)
                .longValue(Bandwidths.BYTES_PER_SECOND);
    }

    /**
     * Calculates rate in Megabits per second between times given where deltaBytes bytes were received (same units as
     * individual request bandwidths on traffic recorder and estimates on traffic logs)
     *
     * @param deltaBytes total number of bytes received between lastTime and time
     * @param time       current (end) time
     * @param lastTime   start time
     * @return rate in Megabits per second
     */
    private double calculateRate(long deltaBytes, long time, long lastTime) {
        double bytesPerNanosecond = (double) (deltaBytes) / (time - lastTime);
        return (Measure.valueOf(bytesPerNanosecond, Bandwidths.BYTES_PER_NANOSECOND))
                .doubleValue(Bandwidths.MEGABITS_PER_SECOND);
    }

    /**
     * Formats important info from update in line json format.
     *
     * @param updateTime      update time at which it is reporting
     * @param totalRateEst    estimated rate between the last updateTime and this one
     * @param actualTotalRate actual rate between the last updateTime and this one
     * @return formatted json
     */
    private String formatOutputJson(long updateTime, double totalRateEst, double actualTotalRate) {
        return String.format("{\"type\":\"totalUpdate\",\"time\":%d,\"estimate\":%f,\"actual\":%f}%n", updateTime,
                totalRateEst, actualTotalRate);
    }

    public static void main(String[] args) {
    }
}
