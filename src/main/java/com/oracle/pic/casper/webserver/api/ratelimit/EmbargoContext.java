package com.oracle.pic.casper.webserver.api.ratelimit;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.identity.authentication.Principal;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An embargo context is very similar to a visa for customs.
 * You must present your visa when you finally decide to leave the country.
 */
@Deprecated
public class EmbargoContext {

    private volatile Principal principal;

    private final String httpMethod;

    private final String userAgent;

    private final String uri;

    private volatile CasperOperation operation;

    private final AtomicBoolean enteredCustoms;

    private final AtomicBoolean exited;

    private volatile boolean allowCustomsReEntry;

    public EmbargoContext(String httpMethod, String uri, String userAgent) {
        this.httpMethod = Preconditions.checkNotNull(httpMethod);
        this.userAgent = Preconditions.checkNotNull(userAgent);
        this.uri = Preconditions.checkNotNull(uri);
        this.enteredCustoms = new AtomicBoolean(false);
        this.exited = new AtomicBoolean(false);
        this.allowCustomsReEntry = false; // default is do not allow re-entry
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public String getUri() {
        return uri;
    }

    public @Nullable Principal getPrincipal() {
        return principal;
    }

    public @Nullable  CasperOperation getOperation() {
        return operation;
    }

    public boolean allowReEntry() {
        return allowCustomsReEntry;
    }

    public void setAllowReEntry(boolean condition) {
        allowCustomsReEntry = condition;
    }

    public boolean hasEnteredCustoms() {
        return enteredCustoms.get();
    }

    public void enterCustoms() {
        enteredCustoms.set(true);
    }

    public boolean hasExited() {
        return exited.get();
    }

    public void exit() {
        exited.compareAndSet(false, true);
    }

    public EmbargoContext setPrincipal(Principal principal) {
        this.principal = Preconditions.checkNotNull(principal);
        return this;
    }

    public EmbargoContext setOperation(CasperOperation operation) {
        this.operation = Preconditions.checkNotNull(operation);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmbargoContext that = (EmbargoContext) o;
        return allowCustomsReEntry == that.allowCustomsReEntry &&
                java.util.Objects.equals(principal, that.principal) &&
                httpMethod == that.httpMethod &&
                java.util.Objects.equals(userAgent, that.userAgent) &&
                java.util.Objects.equals(uri, that.uri) &&
                java.util.Objects.equals(enteredCustoms, that.enteredCustoms) &&
                java.util.Objects.equals(exited, that.exited);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(principal, httpMethod, userAgent, uri,
                enteredCustoms, exited, allowCustomsReEntry);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("principal", principal)
                .add("httpMethod", httpMethod)
                .add("userAgent", userAgent)
                .add("uri", uri)
                .add("enteredCustoms", enteredCustoms)
                .add("exited", exited)
                .add("allowCustomsReEntry", allowCustomsReEntry)
                .toString();
    }
}
