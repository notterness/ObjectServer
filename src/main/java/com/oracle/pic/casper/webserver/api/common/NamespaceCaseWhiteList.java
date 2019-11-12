package com.oracle.pic.casper.webserver.api.common;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;

import com.oracle.pic.casper.objectmeta.Api;

/**
 * To support case insensitivity for namespaces, we have to implement a whitelist to support existing
 * namespaces which contain upper cased letters.
 */
public final class NamespaceCaseWhiteList {

    private NamespaceCaseWhiteList() {
    }

    private static final ImmutableSet<Pair<Api, String>> WHITELIST =
            ImmutableSet.of(
                    Pair.of(Api.V2, "CasperTestR2"),
                    Pair.of(Api.V2, "HoPSTest"),
                    Pair.of(Api.V2, "InternalBrianGustafson"),
                    Pair.of(Api.V1, "Casper-Canary-Scope"),
                    Pair.of(Api.V1, "DBEng"));


    public static String lowercaseNamespace(Api api, String namespace) {
        return WHITELIST.contains(Pair.of(api, namespace)) ? namespace : namespace.toLowerCase();
    }
}
