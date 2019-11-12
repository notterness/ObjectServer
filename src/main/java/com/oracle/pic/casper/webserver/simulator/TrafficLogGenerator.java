package com.oracle.pic.casper.webserver.simulator;

import com.oracle.pic.casper.webserver.traffic.TrafficRecorder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Scanner;


public class TrafficLogGenerator {
    /**
     * Generates traffic log that doesn't include any rate estimates according to the following format:
     * start [request id] [type (PutObject/GetObject)] [time (nanoseconds)] [length (bytes)]
     * update [request rid] [total bytes received] [time (nanoseconds)]
     * end [request rid] [time (nanoseconds)]
     *
     * Main use is to generate/write the input file to the TrafficRecorderSimulator.
     */

    static final long SECOND = 1_000_000_000;
    static final long MICROSECOND = 1_000;


    /**
     * Last request ID that has been used to start a request on the Traffic Recorder, kept so that independent
     * sequential scenarios can be called and graphed against each other.
     */
    private int lastID;


    public TrafficLogGenerator() {
        lastID = 0;
    }

    /**
     * Creates a log that mimics some number of request ID constant transfer(s) of data with given inputs, stops when
     * number of buffer arrivals hits limit.
     *
     * @param type            type of request, GetObject or PutObject
     * @param bytesPerSecond  number of bytes per second that will be simulated in request
     * @param contentLength   length of the request
     * @param startTime       time to start at, should be 0 unless this is called after another method where the
     *                        trafficRecorder has already been used
     * @param arrivalInterval average amount of time after a request finishes until the next arrives
     * @param numRequests     number of buffer arrivals/updates to simulate before stopping simulation
     * @return log that is generated for input to the simulator
     */
    public void constant(TrafficRecorder.RequestType type, long bytesPerSecond, long contentLength, long startTime,
                         long arrivalInterval, int numRequests) {
        long time = startTime;

        for (int i = 0; i < numRequests; i++) {
            final String id = Integer.toString(lastID);
            lastID++;
            System.out.println(String.format("start %s %s %d %d%n", id, type, time, contentLength));
            long bytesSent = 0L;

            while (bytesSent < contentLength) {
                bytesSent = Math.min(bytesSent + bytesPerSecond / 1000000, contentLength);
                time += MICROSECOND;
                System.out.println(String.format("update %s %d %d%n", id, bytesSent, time));
            }
            time += MICROSECOND;
            System.out.println(String.format("end %s %d%n", id, time));
            time += arrivalInterval;
        }
    }

    /**
     * Creates a log that mimics a single stall request ID that sends constant Bps buffers for some amount of time and
     * then stops.
     *
     * @param bytesPerSecond number of bytes per second that will be simulated in request
     * @param contentLength  length of the request
     * @param startTime      time to start at, should be 0 unless this is called after another method where the
     *                       trafficRecorder has already been used
     * @param stallPoint     point at which the request should be stalled (a percent)
     * @param stallDelay     number of timesteps for which the request should stall before finishing
     * @return log that is generated for input to the simulator
     */
    public void stall(long bytesPerSecond, long contentLength, long startTime, double stallPoint, int stallDelay) {
        long time = startTime;
        final String id = Integer.toString(lastID);
        lastID++;

        System.out.println(String.format("start %s %s %d %d%n", id, TrafficRecorder.RequestType.PutObject, time,
                contentLength));
        long bytesSent = 0L;
        int stallCount = 0;

        while (bytesSent < contentLength) {
            if (bytesSent + bytesPerSecond / 1000000 >= stallPoint / 100.0 * contentLength && stallCount < stallDelay) {
                stallCount += 1;
            } else {
                bytesSent = Math.min(bytesSent + bytesPerSecond / 1000000, contentLength);
            }
            time += MICROSECOND;
            System.out.println(String.format("update %s %d %d%n", id, bytesSent, time));
        }
        time += MICROSECOND;
        System.out.println(String.format("end %s %d%n", id, time));
    }

    @SuppressFBWarnings(value = {"PATH_TRAVERSAL_OUT", "PATH_TRAVERSAL_IN"})
    /**
     * Creates a log that mimics numRequests concurrent requests with the given average throughput, constant content
     * length, and constant bufferSize. Writes this to filename. Interarrivals of separate requests are based on an
     * exponential distribution. Throughput rates are based on a normal distribution.
     *
     * @param outputFilename    the file path that the log should be written to
     * @param putLengthsFile    file with content lengths to select from for PUT Requests, should have whole, nonzero
     *                          numbers separated by new lines
     * @param getLengthsFile    file with content lengths to select from for GET Requests, should have whole, nonzero
     *                          numbers separated by new lines
     * @param putCount          ratio of requests that will be PUTs relative to getCounts GETs (i.e., 1 to 1 will
     *                          split approximately equally)
     * @param getCount          ratio of requests that will be GETs relative to putCounts PUTs (i.e., 1 to 1 will
     *                          split approximately equally)
     * @param avgBytesPerSecond average bytes to send per second per request, used to calculate update time of a
     *                          request
     * @param stdvPercent       standard deviation will be calculated by this percent (as a decimal) times the average
     * @param bufferSize        constant buffer size in bytes for requests
     * @param numRequests       total number of requests to send and complete
     * @param avgInterarrival   average number of nanoseconds between the beginning of new requests
     * @param throughputRate    _slow_ throughput rate to be applied to some percent of requests in bytes per second, -1
     *                          for nothing
     * @param throughputPercent percent of requests to slow down to throughputRate, -1 for nothing
     * @param stallPoint        percent point at which request should be stalled, -1 for nothing (i.e., a 50 would mean
     *                          the request stalls right before sending the halfway byte)
     * @param stallDelay        duration of time in nanoseconds that the stall should stall for. this will be in
     *                          addition to the average time between updates calculated from the rate
     * @param stallPercent      percent of requests to stall at stall point
     * @throws IOException if file cannot be found or cannot be written to
     */
    public void concurrent(String outputFilename, String putLengthsFile, String getLengthsFile,
                           int putCount, int getCount, long avgBytesPerSecond,
                           double stdvPercent,
                           long bufferSize, int numRequests, long avgInterarrival, long throughputRate,
                           int throughputPercent, int stallPoint, long stallDelay, int stallPercent)
            throws IOException {

        final Scanner putScanner = new Scanner(new File(putLengthsFile), "UTF-8");
        final ArrayList<Long> putLengths = new ArrayList<>();
        while (putScanner.hasNextLong()) {
            putLengths.add(putScanner.nextLong());
        }
        final Scanner getScanner = new Scanner(new File(getLengthsFile), "UTF-8");
        final ArrayList<Long> getLengths = new ArrayList<>();
        while (getScanner.hasNextLong()) {
            getLengths.add(getScanner.nextLong());
        }

        final OutputStreamWriter writer =
                new OutputStreamWriter(new FileOutputStream(outputFilename), StandardCharsets.UTF_8);
        final SecureRandom random = new SecureRandom();
        final PriorityQueue<LogEvent> queue = new PriorityQueue<>(Comparator.comparingLong(LogEvent::getTime));
        long startTime = 0L;

        for (int i = 0; i < numRequests; i++) {
            String id = Integer.toString(i);
            final boolean isPut = random.nextInt(putCount + getCount) < putCount;
            final long contentLength = randomElement(random, isPut ? putLengths : getLengths);
            queue.add(new LogEvent(LogEventType.START, id, isPut ? TrafficRecorder.RequestType.PutObject :
                    TrafficRecorder.RequestType.GetObject,
                    startTime,
                    contentLength, 0L));

            long updateTime = startTime;
            long bytesSent = 0L;
            long curAvgBytesPerSecond = avgBytesPerSecond;
            if (throughputRate != -1 && random.nextInt(100) < throughputPercent) {
                curAvgBytesPerSecond = throughputRate;
            }
            final long avgUpdateInterval = convertToNanosecondDuration(bufferSize, curAvgBytesPerSecond);
            final boolean shouldStall = stallPoint != -1 && random.nextInt(100) < stallPercent;
            final long stallByte = stallPoint == -1 ? -1 : (long) (stallPoint / 100.0 * contentLength);
            boolean stalled = false;

            while (bytesSent < contentLength) {
                long leftover = contentLength - bytesSent;
                long curAvgUpdateInterval = leftover >= bufferSize ? avgUpdateInterval :
                        convertToNanosecondDuration(leftover, avgBytesPerSecond);
                // 0 update time would show a rate of "infinity"
                updateTime += Math.max(1, gaussian(random, curAvgUpdateInterval, stdvPercent));
                bytesSent = bytesSent + Math.min(bufferSize, leftover);
                if (!stalled && bytesSent >= stallByte && shouldStall) {
                    updateTime += stallDelay;
                    stalled = true;
                }
                queue.add(new LogEvent(LogEventType.UPDATE, id, TrafficRecorder.RequestType.PutObject, updateTime,
                        contentLength, bytesSent));
            }
            updateTime += 1;
            queue.add(new LogEvent(LogEventType.END, id, TrafficRecorder.RequestType.PutObject, updateTime,
                    contentLength, bytesSent));
            startTime += exponential(random, avgInterarrival);
        }
        while (!queue.isEmpty()) {
            writer.write(queue.poll().toString());
        }
        writer.close();
    }

    /**
     * Finds duration of time that should be taken to "send" the buffer given its size and the rate.
     *
     * @param bufferSize     size of the buffer to send in bytes
     * @param bytesPerSecond targetted rate to send the buffer in bytes per second
     * @return sleep duration in nanoseconds
     */
    private static long convertToNanosecondDuration(long bufferSize, long bytesPerSecond) {
        final double bytesPerNanosecond = bytesPerSecond / (double) SECOND;
        final long timeInterval = (long) (bufferSize / bytesPerNanosecond);
        return timeInterval;
    }

    /**
     * Returns a randomly generated number with the normal distribution having average average and standard deviation
     * stdvPercent*average. Random().nextGaussian() produces a normally distributed number with a standard deviation of
     * 1 and average of 0. We can scale this number by the standard deviation and shift it by the average to get a
     * number with the distribution we want.
     *
     * @param average     mean of the normal distribution
     * @param stdvPercent used to calculate the standard deviation by multiplying by the average
     * @return random normally distributed number with above parameters
     */
    private static long gaussian(Random random, long average, double stdvPercent) {
        final long stdv = (long) (stdvPercent * average);
        return (long) (random.nextGaussian() * stdv) + average;
    }

    /**
     * Returns a randomly generated number from an exponential distribution with average average. We use the inversion
     * method by generating a uniform number across [0,1) and finding its x component on an exponential graph. Lambda is
     * calculated by (1/average). The equation is then x = log(1-u)/(-lambda), as explained at
     * https://stackoverflow.com/questions/2106503/pseudorandom-number-generator-exponential-distribution.
     *
     * @param average mean of the exponential distribution
     * @return random exponentially distributed number with above average
     */
    public static double exponential(Random random, long average) {
        return Math.log(1 - random.nextDouble()) / (-1.0 / average);
    }

    public static long randomElement(Random random, ArrayList<Long> items) {
        return items.get(random.nextInt(items.size()));
    }

    public static void main(String[] args) {
    }

}
