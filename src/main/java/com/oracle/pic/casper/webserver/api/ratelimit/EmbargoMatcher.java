package com.oracle.pic.casper.webserver.api.ratelimit;

import com.oracle.pic.casper.mds.operator.MdsEmbargoRuleV1;

import java.util.function.Predicate;
import javax.annotation.Nullable;

@Deprecated
public final class EmbargoMatcher {

    private EmbargoMatcher() {
    }

    static Predicate<String> isExactMatchCaseInsensitive(String str) {
        return (v) -> v != null && v.equalsIgnoreCase(str);
    }

    static Predicate<String> isExactMatchCaseSensitive(String str) {
        return (v) -> v != null && v.equals(str);
    }

    static Predicate<String> isTenantId(String tenantId) {
        return (t) -> t != null && t.equalsIgnoreCase(tenantId);
    }

    static Predicate<String> isHttpMethod(String method) {
        return (m) -> isExactMatchCaseInsensitive(m).test(method);
    }

    static Predicate<String> isPrefix(String value) {
        return (s) -> s != null && s.startsWith(value);
    }

    public static boolean matches(
        MdsEmbargoRuleV1 rule,
        @Nullable String inOperation,
        @Nullable String inTenantId,
        @Nullable String inUri,
        @Nullable String inMethod,
        @Nullable String inAgent) {
        boolean methodMatches =
            rule.getHttpMethod().isEmpty() || EmbargoMatcher.isHttpMethod(rule.getHttpMethod()).test(inMethod);
        boolean verbMatches = rule.getOperation().isEmpty() ||
            EmbargoMatcher.isExactMatchCaseInsensitive(rule.getOperation()).test(inOperation);
        boolean userAgentMatches =
            rule.getUserAgent().isEmpty() || EmbargoMatcher.isPrefix(rule.getUserAgent()).test(inAgent);
        boolean uriMatches = rule.getUriPrefix().isEmpty() || EmbargoMatcher.isPrefix(rule.getUriPrefix()).test(inUri);
        boolean tenantMatches =
            rule.getTenantId().isEmpty() || EmbargoMatcher.isTenantId(rule.getTenantId()).test(inTenantId);
        return rule.getEnabled() && methodMatches && verbMatches && uriMatches && tenantMatches && userAgentMatches;
    }
}
