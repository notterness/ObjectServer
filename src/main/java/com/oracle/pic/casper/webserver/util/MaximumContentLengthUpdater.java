package com.oracle.pic.casper.webserver.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.webserver.traffic.KeyValueStoreUpdater;
import com.oracle.pic.casper.webserver.api.common.CountingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.measure.Measure;
import javax.measure.quantity.DataAmount;
import java.util.Map;
import java.util.Set;

public class MaximumContentLengthUpdater implements KeyValueStoreUpdater.Listener {
    private static final Logger LOG = LoggerFactory.getLogger(MaximumContentLengthUpdater.class);

    private static final String KEY_PREFIX = "SBH";
    static final String MAXIMUM_CONTENT_LENGTH_KEY = String.format("%s|MAXIMUM_CONTENT_LENGTH", KEY_PREFIX);

    private static final ObjectMapper MAPPER = CasperConfig.getObjectMapper();

    private final CountingHandler.MaximumContentLength maximumContentLength;
    private final Measure<DataAmount> defaultMaximum;

    public MaximumContentLengthUpdater(CountingHandler.MaximumContentLength maximumContentLength) {
        this.maximumContentLength = maximumContentLength;
        this.defaultMaximum = maximumContentLength.getMaximum();
    }

    private String jsonString(String value) {
        return String.format("\"%s\"", value);
    }

    @Override
    public void update(Map<String, String> changed, Set<String> removed) {
        String value = changed.get(MAXIMUM_CONTENT_LENGTH_KEY);
        if (removed.contains(MAXIMUM_CONTENT_LENGTH_KEY)) {
            maximumContentLength.setMaximum(defaultMaximum);
        } else if (value != null) {
            try {
                Measure<DataAmount> newMaximum = MAPPER.readValue(
                        jsonString(value),
                        new TypeReference<Measure<DataAmount>>() {
                        }
                );
                maximumContentLength.setMaximum(newMaximum);
            } catch (Exception e) {
                LOG.debug("received a bad value for key {}: {}", MAXIMUM_CONTENT_LENGTH_KEY, value);
                WebServerMetrics.SBH_UPDATE_ERROR.inc();
            }
        }
    }
}
