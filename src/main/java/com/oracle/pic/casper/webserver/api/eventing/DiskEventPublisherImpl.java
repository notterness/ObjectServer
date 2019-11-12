package com.oracle.pic.casper.webserver.api.eventing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.oracle.bmc.model.BmcException;
import com.oracle.pic.casper.common.config.v2.EventServiceConfiguration;
import com.oracle.pic.casper.common.json.JacksonSerDe;
import com.oracle.pic.casper.common.json.JsonDeserializationException;
import com.oracle.pic.casper.common.json.JsonSerializationException;
import com.oracle.pic.casper.common.model.LogFile;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.webserver.auth.AuthTestConstants;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.events.EventsIngestionClient;
import com.oracle.pic.events.model.BaseEvent;
import com.oracle.pic.events.model.EventV01;
import com.oracle.pic.events.model.FailedEventRecord;
import com.oracle.pic.events.model.PublishEventsDetails;
import com.oracle.pic.events.model.PublishEventsResponseDetails;
import com.oracle.pic.events.requests.PublishEventsRequest;
import com.oracle.pic.events.responses.PublishEventsResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Class that persist events to disk as they happen and runs a background thread that processes the on disk files
 * periodically to send to the events service
 *
 * TODO This class shares a lot of similarities with the metering service, some refactoring is needed in the future
 */
@SuppressFBWarnings("PATH_TRAVERSAL_IN")
public class DiskEventPublisherImpl extends Thread implements EventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(DiskEventPublisherImpl.class);

    private static final Pattern VALID_BUCKET_NAME = Pattern.compile("[A-Za-z0-9\\-_\\.]+");

    private static final String DATE_GROUP = "date";
    private static final String COUNTER_GROUP = "counter";
    // number of times to retry if file processing fails
    private static final int FILE_PROCESS_RETRY = 5;

    //a dummy log message to force rollover of old events when there is no new events
    private static final String DUMMY_MESSAGE = "dummy message";

    private final AtomicBoolean isRunning;
    private final Logger logger;
    private final JacksonSerDe jacksonSerDe;
    private final Pattern filePattern;
    private final Path logDirectory;
    private final Path stateFile;
    private final int batchSize;
    private final EventsIngestionClient eventsClient;
    private final Limits limits;
    private final Set<String> whitelist;
    private final Map<String, Integer> brokenFiles;
    private final EventServiceConfiguration config;
    private final RetryPolicy retryPolicy;

    public DiskEventPublisherImpl(EventServiceConfiguration config,
                                  EventsIngestionClient eventsClient,
                                  JacksonSerDe jacksonSerDe,
                                  Limits limits) {
        this(config, eventsClient, System.getProperty("user.dir"), jacksonSerDe,  limits);
    }

    DiskEventPublisherImpl(EventServiceConfiguration config,
                           EventsIngestionClient eventsClient,
                           String workingDir,
                           JacksonSerDe jacksonSerDe,
                           Limits limits) {
        this.config = config;
        this.logger = LoggerFactory.getLogger("com.oracle.pic.casper.event.logging");
        this.eventsClient = eventsClient;
        this.isRunning = new AtomicBoolean(true);
        this.jacksonSerDe = jacksonSerDe;
        this.filePattern = Pattern.compile(config.getFilePattern());
        this.logDirectory = Paths.get(workingDir, config.getLogDirectory());
        this.stateFile = Paths.get(workingDir, config.getLogDirectory(), config.getStateFile());
        this.batchSize = config.getBatchSize();
        this.brokenFiles = new TreeMap<>();
        this.limits = limits;
        this.whitelist = limits != null ? new HashSet<>() : config.getWhitelist();
        this.whitelist.addAll(Arrays.asList(config.getEnvVarWhitelistAddition().split(",")));
        this.retryPolicy = new RetryPolicy().retryOn(BmcException.class)
                .withBackoff(config.getRetryConfig().getMinRetryDelayInMs(),
                        config.getRetryConfig().getMaxRetryDelayInMs(),
                        TimeUnit.MILLISECONDS)
                .withMaxRetries(config.getRetryConfig().getMaxRetries())
                .withMaxDuration(5, TimeUnit.SECONDS);
        this.setName("diskBasedEventPublisher");
    }

    @Override
    public void addEvent(CasperEvent event, String tenantId, @Nullable ObjectLevelAuditMode auditMode) {
        try {
            if (shouldPublish(event, tenantId, auditMode)) {
                logger.info("{}", jacksonSerDe.toJson(event.toCloudEvent()));
            }
        } catch (Exception e) {
            WebServerMetrics.EVENTS_FAILED_TO_ADD.inc();
            LOG.error("Failed to log event", e);
        }
    }

    @Override
    public void run() {
        if (!config.useOldPublisher()) {
            logger.info(DUMMY_MESSAGE);
        }
        final long intervalMs = config.getPublishInterval().toMillis();
        final long intervalJitterMs = config.getPublishIntervalJitter().toMillis();
        while (isRunning.get()) {
            try {
                Thread.sleep(intervalMs + RandomUtils.nextLong(0, intervalJitterMs));
                oneIteration();
            } catch (Exception e) {
                LOG.info("Exception during event publisher iteration", e);
                WebServerMetrics.EVENTS_UNCAUGHT_ERROR.inc();
            }
        }
        LOG.info("Shutting down disk event publisher");
    }

    /**
     * maybe one day we will split this out into it's own service like metering agent
     */
    @Override
    public void stopRunning() {
        this.isRunning.set(false);
    }

    @VisibleForTesting
    void oneIteration() {
        WebServerMetrics.EVENTS_NUM_BROKEN_FILE.set(brokenFiles.size());
        Collection<File> files = FileUtils.listFiles(logDirectory.toFile(), null, false);
        // time of the last log file to be processed, only process the files after this timestamp
        LogFile lastProcessEventLog = getLastProcessEventLog();
        LogFile nextInLineOpt = getNextLogFile(files, lastProcessEventLog);
        if (nextInLineOpt != null) {
            processFile(nextInLineOpt);
            WebServerMetrics.OLDEST_EVENT_AGE_SEC.set(
                Duration.between(nextInLineOpt.getDate(), Instant.now()).get(ChronoUnit.SECONDS));
        } else {
            WebServerMetrics.OLDEST_EVENT_AGE_SEC.set(0);
        }
    }

    @VisibleForTesting
    LogFile getLastProcessEventLog() {
        if (!stateFile.toFile().exists()) {
            return null;
        }

        try {
            String stateStr = FileUtils.readFileToString(stateFile.toFile(), "utf-8");
            if (stateStr.isEmpty()) {
                return null;
            }
            LogFile state = jacksonSerDe.fromJson(stateStr, LogFile.class);
            return state;
        } catch (IOException | JsonSerializationException e) {
            // if we are unable to read the current log state just return Instant.MIN to scan all the
            // available logs. The event logs messages include an UUID that events service can use to dedup.
            LOG.warn("Failed to read event log state, failing back to processing all available logs", e);
        }
        return null;
    }

    @VisibleForTesting
    void updateEventLogStatus(LogFile logFile) {
        try {
            // this does not flush the output stream to disk but we don't really care if the state file is
            // broken some times since we'll just reprocess some old files,
            // the items in the files have UUID that allows event service to deduplicate
            Files.write(stateFile,
                        jacksonSerDe.toJson(logFile).getBytes("UTF-8"),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.CREATE);
        } catch (IOException ioe) {
            LOG.warn("Failed to updated event log state, next iteration will likely process old files", ioe);
        }
    }

    @VisibleForTesting
    LogFile getNextLogFile(Collection<File> files, LogFile lastProcessed) {
        File oldestUnprocessedFile = null;
        Instant oldestUnprocessedFileTime = Instant.MAX;
        int oldestUnprocessedFileCounter = 0;

        Instant cutoffTime = lastProcessed == null ? Instant.MIN : lastProcessed.getDate();
        int cutOffCounter = lastProcessed == null ? 0 : lastProcessed.getId();

        for (File f : files) {
            if (brokenFiles.getOrDefault(f.getAbsolutePath(), 0) > FILE_PROCESS_RETRY) {
                continue;
            }
            Matcher matcher = filePattern.matcher(f.getName());
            if (matcher.matches()) {
                Instant curTs = Instant.parse(matcher.group(DATE_GROUP));
                int curCounter = Integer.parseInt(matcher.group(COUNTER_GROUP));

                // is current file after the cutoff time
                if (curTs.isAfter(cutoffTime) || (curTs.equals(cutoffTime) && curCounter > cutOffCounter)) {
                    // is current file before the current oldest unprocessed file
                    if (curTs.isBefore(oldestUnprocessedFileTime) ||
                        (curTs.equals(oldestUnprocessedFileTime) && curCounter < oldestUnprocessedFileCounter)) {
                        oldestUnprocessedFile = f;
                        oldestUnprocessedFileTime = curTs;
                        oldestUnprocessedFileCounter = curCounter;
                    }
                }
            }
        }

        if (oldestUnprocessedFile == null) {
            return null;
        }
        return new LogFile(oldestUnprocessedFile, oldestUnprocessedFileTime, oldestUnprocessedFileCounter);
    }

    @VisibleForTesting
    void processFile(LogFile logFile) {
        File file = logFile.getFile();
        LOG.info("Processing {}", file.getAbsolutePath());
        long eventCounter = 0;
        try (BufferedReader br =
                 new BufferedReader(
                     new InputStreamReader(
                         new GZIPInputStream(Files.newInputStream(file.toPath(), StandardOpenOption.READ)), "UTF-8"))) {
            String line;
            long lineNum = 1;
            List<BaseEvent> batch = new LinkedList<>();
            while ((line = br.readLine()) != null) {
                try {
                    if (line.contains(DUMMY_MESSAGE)) {
                        continue;
                    }
                    EventV01 event = jacksonSerDe.fromJson(line, EventV01.class);
                    batch.add(event);
                    if (batch.size() >= batchSize) {
                        sendBatch(batch);
                        eventCounter += batch.size();
                        batch = new LinkedList<>();
                    }
                } catch (JsonDeserializationException | JsonSerializationException e) {
                    LOG.error("Failed read event from file {} line {}", file.getName(), lineNum, e);
                } finally {
                    lineNum++;
                }
            }
            // send the last batch if these is anything to send
            if (batch.size() > 0) {
                sendBatch(batch);
                eventCounter += batch.size();
            }
            LOG.info("Processed {} events from {}", eventCounter, file.getAbsolutePath());
        } catch (IOException ioe) {
            brokenFiles.putIfAbsent(file.getAbsolutePath(), 0);
            brokenFiles.computeIfPresent(file.getAbsolutePath(), (k, v) -> v + 1);
            LOG.warn("Failed to process {}", file.getName(), ioe);
            return;
        }

        updateEventLogStatus(logFile);
    }

    @VisibleForTesting
    void sendBatch(List<BaseEvent> batch) {
        // hack around the fact that we need to retry only the failed events
        // events service throttled per event instead of per batch request
        final List<BaseEvent> eventsToSend = new ArrayList<>(batch);

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        Failsafe.with(retryPolicy).onFailedAttempt(x -> {
            WebServerMetrics.EVENT_SERVICE_CALL.getOverallLatency()
                .update(stopwatch.stop().elapsed(TimeUnit.NANOSECONDS));
            stopwatch.reset();
        }).run(() -> {
            PublishEventsDetails payload = PublishEventsDetails.builder().events(eventsToSend).build();
            PublishEventsRequest batchEventRequest = PublishEventsRequest.builder()
                    .events(payload)
                    .opcRequestId(UUID.randomUUID().toString())
                    .build();
            WebServerMetrics.EVENT_SERVICE_CALL.getRequests().inc();
            WebServerMetrics.EVENTS_SENT.inc(eventsToSend.size());
            stopwatch.start();
            PublishEventsResponse response = eventsClient.publishEvents(batchEventRequest);
            Set<String> toRetry = getRetryableEvents(response);
            WebServerMetrics.EVENTS_SENT_SUCCESS.inc(
                    eventsToSend.size() - response.getPublishEventsResponseDetails().getFailedEvents().size());
            if (!toRetry.isEmpty()) {
                int totalSent = eventsToSend.size();
                int i = 0;
                while (i < eventsToSend.size()) {
                    if (!toRetry.contains(((EventV01) eventsToSend.get(i)).getEventID())) {
                        eventsToSend.remove(i);
                    } else {
                        i++;
                    }
                }
                throw new BmcException(500,
                        "too many events failed in the batch",
                        String.format("%d out of %d events needs to be retried", toRetry.size(), totalSent),
                        response.getOpcRequestId());
            }
            WebServerMetrics.EVENT_SERVICE_CALL.getOverallLatency()
                .update(stopwatch.stop().elapsed(TimeUnit.NANOSECONDS));
            stopwatch.reset();
            WebServerMetrics.EVENT_SERVICE_CALL.getSuccesses().inc();
        });
    }

    /**
     * TODO improve error handling, need documentation from events team on final set of possible response codes
     * for the batch request, for now do the naive thing and just resend the batch since each event has an unique ID
     */
    private Set<String> getRetryableEvents(PublishEventsResponse response) {
        PublishEventsResponseDetails responseDetails = response.getPublishEventsResponseDetails();
        // TODO follow up with events team on what are the actual errors that will be thrown,
        // as of 09/05/2018 schema validation isn't in place so assuming all errors are retryable until told otherwise
        // by events team
        Set<String> result = new HashSet<>();
        if (!responseDetails.getFailedEvents().isEmpty()) {
            WebServerMetrics.EVENT_SERVICE_CALL.getServerErrors().inc();
            // if everything has failed then throw an exception and retry
            for (FailedEventRecord r : responseDetails.getFailedEvents()) {
                if (canRetryEvent(r)) {
                    result.add(r.getEventID());
                }
            }
        }
        return result;
    }

    /**
     * Only V2 API events are published.
     * Object events are published if the tenant is whitelisted.
     * The same event can also already be emitted from OAL if auditMode is Write or ReadWrite.  In that case, no need to
     * publish again here.
     */
    @VisibleForTesting
    boolean shouldPublish(CasperEvent event, String tenantId, ObjectLevelAuditMode auditMode) {
        if (!config.useOldPublisher()) {
            return false;
        }
        Preconditions.checkArgument(event instanceof CasperObjectEvent);
        //no event for V1 or invalid buckets
        if (event.getApi() == Api.V1 || !isValidBucket(event.getBucketName())) {
            return false;
        }
        //check if tenant is whitelisted
        final boolean tenantIsWhitelisted;
        if (AuthTestConstants.FAKE_TENANT_ID.equals(tenantId)) {
            tenantIsWhitelisted = true;
        } else if (limits != null) {
            tenantIsWhitelisted = limits.getEventsWhitelistedTenancies().contains(tenantId);
        } else {
            tenantIsWhitelisted = whitelist.contains(tenantId);
        }
        if (!tenantIsWhitelisted) {
            return false;
        }
        //if we have already emitted an event via OAL, do not duplicate it
        final boolean alreadyEmittedEvent = auditMode == ObjectLevelAuditMode.ReadWrite ||
            auditMode == ObjectLevelAuditMode.Write;
        return !alreadyEmittedEvent;
    }

    @VisibleForTesting
    static boolean canRetryEvent(FailedEventRecord r) {
        return r.getCode().equals("429") || r.getCode().startsWith("5") && !r.getCode().equals("501");
    }

    @VisibleForTesting
    static boolean isValidBucket(String bucketName) {
        // This validation has been borrowed from Validator class in Webserver Util
        return VALID_BUCKET_NAME.matcher(bucketName).matches();
    }
}
