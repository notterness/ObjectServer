package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Objects;

@Immutable
public class ObjectLifecycleRule {
    private final String name;
    private final String action;
    private final boolean isEnabled;
    private final LifecycleRuleFilter objectNameFilter;
    private final ObjectLifecycleTimeUnit timeUnit;
    private final Long timeAmount;

    @JsonCreator
    public ObjectLifecycleRule(@JsonProperty("name") String name,
                               @JsonProperty("action") String action,
                               @JsonProperty("timeAmount") long timeAmount,
                               @JsonProperty("timeUnit") ObjectLifecycleTimeUnit timeUnit,
                               @JsonProperty("isEnabled") boolean isEnabled,
                               @Nullable @JsonProperty("objectNameFilter") LifecycleRuleFilter objectNameFilter) {
        this.name = name;
        this.action = action;
        this.isEnabled = isEnabled;
        this.objectNameFilter = objectNameFilter;
        this.timeAmount = timeAmount;
        this.timeUnit = timeUnit;
    }

    public static final class LifecycleRuleFilter {

        private final List<String> inclusionPrefixes;
        private final List<String> inclusionPatterns;
        private final List<String> exclusionPatterns;

        public LifecycleRuleFilter(@JsonProperty("inclusionPrefixes") List<String> inclusionPrefixes,
                                   @JsonProperty("inclusionPatterns") List<String> inclusionPatterns,
                                   @JsonProperty("exclusionPatterns") List<String> exclusionPatterns) {
            this.inclusionPrefixes = inclusionPrefixes;
            this.inclusionPatterns = inclusionPatterns;
            this.exclusionPatterns = exclusionPatterns;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> getInclusionPrefixes() {
            return inclusionPrefixes;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> getInclusionPatterns() {
            return inclusionPatterns;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> getExclusionPatterns() {
            return exclusionPatterns;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LifecycleRuleFilter that = (LifecycleRuleFilter) o;
            return Objects.equals(inclusionPrefixes, that.inclusionPrefixes) &&
                    Objects.equals(inclusionPatterns, that.inclusionPatterns) &&
                    Objects.equals(exclusionPatterns, that.exclusionPatterns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inclusionPrefixes, inclusionPatterns, exclusionPatterns);
        }
    }

    public String getName() {
        return name;
    }

    public String getAction() {
        return action;
    }

    public long getTimeAmount() {
        return timeAmount;
    }

    public ObjectLifecycleTimeUnit getTimeUnit() {
        return timeUnit;
    }

    public boolean getIsEnabled() {
        return isEnabled;
    }

    public LifecycleRuleFilter getObjectNameFilter() {
        return objectNameFilter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ObjectLifecycleRule)) return false;
        ObjectLifecycleRule that = (ObjectLifecycleRule) o;
        return isEnabled == that.isEnabled &&
                Objects.equals(name, that.name) &&
                action.equals(that.action) &&
                timeAmount.equals(that.timeAmount) &&
                Objects.equals(objectNameFilter, that.objectNameFilter) &&
                Objects.equals(timeUnit, that.timeUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, action, isEnabled, objectNameFilter, timeAmount, timeUnit);
    }

    @Override
    public String toString() {
        return "ObjectLifecycleRule{" +
                "name='" + name + '\'' +
                ", action='" + action + '\'' +
                ", isEnabled=" + isEnabled +
                ", objectNameFilter=" + objectNameFilter +
                ", timeUnit=" + timeUnit +
                ", timeAmount=" + timeAmount +
                '}';
    }
}
