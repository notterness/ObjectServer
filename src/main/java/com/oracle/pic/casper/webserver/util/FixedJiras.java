package com.oracle.pic.casper.webserver.util;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class FixedJiras {

    public enum Subset {
        DEFAULT("FIXED_JIRAS"),
        WITH_EAGLE("EAGLE_FIXED_JIRAS");

        private final String resourceName;

        Subset(String resourceName) {
            this.resourceName = resourceName;
        }

        private List<String> load() {
            final URL jirasResourceUrl = Resources.getResource(resourceName);
            try {
                return Resources.readLines(jirasResourceUrl, StandardCharsets.UTF_8)
                        .stream()
                        .map(String::trim)
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private FixedJiras() {
    }

    /**
     * Return the known list of fixed-jiras that this codebase supports by reading the resource files
     * specified by the {@link FixedJiras.Subset} of fixed JIRAs.
     *
     * Always includes {@link FixedJiras.Subset#DEFAULT}.
     *
     * This file has unit tests to make sure it contains no duplicates and follows a strict naming convention.
     */
    public static List<String> getFixedJiras(Subset... jiraSets) {
        final Set<String> supersetOfJIRAs = new HashSet<>(Subset.DEFAULT.load());
        for (Subset jiraSet : jiraSets) {
            supersetOfJIRAs.addAll(jiraSet.load());
        }
        final List<String> allFixedJIRAs = new ArrayList<>(supersetOfJIRAs);
        Collections.sort(allFixedJIRAs);
        return allFixedJIRAs;
    }
}
