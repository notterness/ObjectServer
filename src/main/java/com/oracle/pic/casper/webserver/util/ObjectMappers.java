package com.oracle.pic.casper.webserver.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableSet;
import com.oracle.bmc.http.internal.ExplicitlySetFilter;
import com.oracle.pic.casper.common.exceptions.ETagMismatchException;
import com.oracle.pic.casper.common.exceptions.IfMatchException;
import com.oracle.pic.casper.common.exceptions.IfNoneMatchException;
import com.oracle.pic.casper.common.exceptions.MultipartUploadNotFoundException;
import com.oracle.pic.casper.common.exceptions.NotFoundException;
import com.oracle.pic.casper.common.exceptions.TooBusyException;
import com.oracle.pic.casper.common.json.CasperThrowableSerializer;
import com.oracle.pic.casper.common.json.MetricScopePropertyFilter;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchBucketException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchObjectException;
import io.vertx.core.VertxException;

import java.util.concurrent.CompletionException;
import java.util.function.Predicate;

/**
 * A helper to configure and create a Jackson ObjectMapper with our custom serialization logic.
 */
public final class ObjectMappers {
    static final ImmutableSet<Class<?>> IGNORE_CLASSES;

    static {
        ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
        builder.add(NoSuchObjectException.class);
        builder.add(NoSuchBucketException.class);
        builder.add(MultipartUploadNotFoundException.class);
        builder.add(ETagMismatchException.class);
        builder.add(IfMatchException.class);
        builder.add(IfNoneMatchException.class);
        builder.add(NotFoundException.class);
        builder.add(TooBusyException.class);
        IGNORE_CLASSES = builder.build();
    }

    private static final Predicate<Throwable> IS_CONNECTION_CLOSED = (throwable) ->
            throwable instanceof CompletionException &&
                    throwable.getCause() instanceof VertxException &&
                    throwable.getCause().getMessage().contains("Connection was closed");

    private static final Predicate<Throwable> IS_IGNORED_CLASS = (throwable -> {
        Throwable t = throwable;
        while (t != null) {
            if (IGNORE_CLASSES.contains(t.getClass())) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    });

    private static final Predicate<Throwable> OMIT = IS_IGNORED_CLASS.or(IS_CONNECTION_CLOSED);

    static Boolean shouldOmitThrowableDetailFunc(Throwable throwable) {
        return OMIT.test(throwable);
    }

    private ObjectMappers() {

    }

    public static ObjectMapper createCasperObjectMapper() {
        SimpleFilterProvider simpleFilterProvider = new SimpleFilterProvider();
        simpleFilterProvider.addFilter("explicitlySetFilter", ExplicitlySetFilter.INSTANCE);

        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule().addSerializer(
                        Throwable.class,
                        new CasperThrowableSerializer(null, ObjectMappers::shouldOmitThrowableDetailFunc)))
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
                .disable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
                .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                .setFilterProvider(simpleFilterProvider);

        MetricScopePropertyFilter.setFilter(objectMapper);

        return objectMapper;
    }
}
