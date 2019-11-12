package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.accounts.api.PropertiesRuntime;
import com.oracle.pic.accounts.model.CacheValue;
import com.oracle.pic.accounts.model.TagLimit;
import com.oracle.pic.commons.client.model.WithHeaders;

import java.util.List;

/**
 * A fake properties runtime client used for integration testing.
 *
 * The only method currently used by the Casper web server is "isSuspended", which always returns false in this
 * implementation.
 */
public class FakePropertiesRuntimeClient implements PropertiesRuntime {
    private boolean isSuspended;

    public FakePropertiesRuntimeClient() {
        isSuspended = false;
    }
    @Override
    public void close() throws Exception {

    }

    public void setIsSuspended(boolean flag) {
        isSuspended = flag;
    }

    @Override
    public Boolean evaluateLimit(
            String s, String s1, String s2, String s3, String s4, String s5, String s6, String s7) {
        return false;
    }

    @Override
    public WithHeaders<Boolean> evaluateLimitWithHeaders(
            String s, String s1, String s2, String s3, String s4, String s5, String s6, String s7) {
        return null;
    }

    @Override
    public Boolean evaluateProperty(
            String s, String s1, String s2, String s3, String s4, String s5, String s6, String s7) {
        return false;
    }

    @Override
    public WithHeaders<Boolean> evaluatePropertyWithHeaders(
            String s, String s1, String s2, String s3, String s4, String s5, String s6, String s7) {
        return null;
    }

    @Override
    public String getConsoleWhitelistValue(
            String s, String s1, String s2, String s3) {
        return null;
    }

    @Override
    public WithHeaders<String> getConsoleWhitelistValueWithHeaders(String s, String s1, String s2, String s3) {
        return null;
    }

    @Override
    public String getConsoleWhitelists(String s, String s1, String s2) {
        return null;
    }

    @Override
    public WithHeaders<String> getConsoleWhitelistsWithHeaders(String s, String s1, String s2) {
        return null;
    }

    @Override
    public CacheValue getLimitValue(String s, String s1, String s2, String s3, String s4, String s5, String s6) {
        return null;
    }

    @Override
    public WithHeaders<CacheValue> getLimitValueWithHeaders(
            String s, String s1, String s2, String s3, String s4, String s5, String s6) {
        return null;
    }

    @Override
    public List<TagLimit> getLimits(String s, String s1, String s2, String s3, String s4, String s5, String s6) {
        return null;
    }

    @Override
    public WithHeaders<List<TagLimit>> getLimitsWithHeaders(
            String s, String s1, String s2, String s3, String s4, String s5, String s6) {
        return null;
    }

    @Override
    public CacheValue getPropertyValue(String s, String s1, String s2, String s3, String s4, String s5, String s6) {
        return null;
    }

    @Override
    public WithHeaders<CacheValue> getPropertyValueWithHeaders(
            String s, String s1, String s2, String s3, String s4, String s5, String s6) {
        return null;
    }

    @Override
    public Boolean getSuspendedState(String s, String s1, String s2) {
        return isSuspended;
    }

    @Override
    public WithHeaders<Boolean> getSuspendedStateWithHeaders(String s, String s1, String s2) {
        return null;
    }

    @Override
    public List<String> getVisibleTemplates(String s, String s1) {
        return null;
    }

    @Override
    public WithHeaders<List<String>> getVisibleTemplatesWithHeaders(String s, String s1) {
        return null;
    }
}
