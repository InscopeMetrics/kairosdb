/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.reporting;

import com.arpnetworking.commons.jackson.databind.module.BuilderModule;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.impl.TsdMetricsFactory;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.incubator.impl.TsdPeriodicMetrics;
import com.arpnetworking.metrics.jvm.JvmMetricsRunnable;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.google.inject.servlet.ServletModule;
import org.kairosdb.core.http.MonitorFilter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricReportingModule extends ServletModule {
    private static final JavaPropsMapper PROPERTIES_MAPPER = new JavaPropsMapper();
    private static final String PERIOD_KEY = "kairosdb.reporter.period";
    private static final String SINKS_KEY = "kairosdb.reporter.sinks";
    private static final String SERVICE_TAG_KEY = "kairosdb.reporter.service";
    private static final String CLUSTER_TAG_KEY = "kairosdb.reporter.cluster";
    private static final String JVM_PERIOD_KEY = "kairosdb.reporter.jvm_period";
    private static final String TAGGER_KEY_PREFIX = "kairosdb.reporter.tagger.";
    private static final String TAGGER_CLASS_SUFFIX = ".class";

    static {
        // Shamelessly copied from ArpNetworking commons:
        // src/main/java/com/arpnetworking/commons/jackson/databind/ObjectMapperFactory.java
        // TODO(Ville): Replace Gson with Jackson throughout KDB and use injected ObjectMapper
        PROPERTIES_MAPPER.registerModule(new BuilderModule());
        PROPERTIES_MAPPER.registerModule(new Jdk8Module());
        PROPERTIES_MAPPER.registerModule(new JavaTimeModule());
        PROPERTIES_MAPPER.registerModule(new GuavaModule());
        PROPERTIES_MAPPER.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);
        PROPERTIES_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        PROPERTIES_MAPPER.configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false);
        PROPERTIES_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        PROPERTIES_MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        PROPERTIES_MAPPER.configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false);
        PROPERTIES_MAPPER.setDateFormat(new StdDateFormat());
    }

    private final Properties properties;

    public MetricReportingModule(final Properties properties) {
        this.properties = properties;
    }

    static Map<String, Class<?>> parseTaggerBuilders(final Properties properties) {
        final Map<String, Class<?>> taggerBuilders = Maps.newHashMap();
        for (final String key : properties.stringPropertyNames()) {
            if (key.startsWith(TAGGER_KEY_PREFIX) && key.endsWith(TAGGER_CLASS_SUFFIX)) {
                final String taggerImplAsString = properties.getProperty(key);
                try {
                    @SuppressWarnings("unchecked") final Class<?> taggerBuilderClass = Class.forName(taggerImplAsString.trim() + "$Builder");
                    taggerBuilders.put(
                            key.substring(0, key.length() - TAGGER_CLASS_SUFFIX.length())
                                    .substring(TAGGER_KEY_PREFIX.length()),
                            taggerBuilderClass);
                } catch (final ClassNotFoundException e) {
                    throw new RuntimeException("Tagger class not found: " + taggerImplAsString, e);
                }
            }
        }
        return taggerBuilders;
    }

    static Map<String, Tagger> loadTaggers(final Map<String, Class<?>> taggerBuilders, final Properties properties) {
        final JavaPropsSchema schema = new JavaPropsSchema()
                .withFirstArrayOffset(0);
        final Map<String, Tagger> taggers = Maps.newHashMap();
        for (final Map.Entry<String, Class<?>> entry : taggerBuilders.entrySet()) {
            final String taggerKey = entry.getKey();
            final Class<?> builderClass = entry.getValue();
            try {
                final Object builder = PROPERTIES_MAPPER.readPropertiesAs(
                        properties,
                        schema.withPrefix(TAGGER_KEY_PREFIX + taggerKey),
                        builderClass);

                @SuppressWarnings("unchecked") final Tagger tagger =
                        (org.kairosdb.core.reporting.Tagger) builderClass.getMethod("build").invoke(builder);
                taggers.put(taggerKey, tagger);
            } catch (final Exception e) {
                throw new RuntimeException(String.format("Unable to create tagger %s from %s", builderClass, taggerKey), e);
            }
        }
        return taggers;
    }

    static List<Class<?>> parseSinkBuilders(final String sinkImplsAsString) {
        final List<Class<?>> sinkBuilders = new ArrayList<>();
        for (final String sinkImplAsString : sinkImplsAsString.split(",")) {
            if (sinkImplAsString.trim().isEmpty()) {
                continue;
            }
            try {
                @SuppressWarnings("unchecked") final Class<?> sinkBuilderClass = Class.forName(sinkImplAsString.trim() + "$Builder");
                sinkBuilders.add(sinkBuilderClass);
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException("Sink class not found: " + sinkImplAsString, e);
            }
        }
        return sinkBuilders;
    }

    static Map<String, Sink> loadSinks(final List<Class<?>> sinkBuilders, final Properties properties) {
        final JavaPropsSchema schema = new JavaPropsSchema()
                .withFirstArrayOffset(0);
        final Map<String, Sink> sinks = Maps.newHashMap();
        for (int i = 0; i < sinkBuilders.size(); ++i) {
            final Class<?> builderClass = sinkBuilders.get(i);
            final String sinkKey = SINKS_KEY + "." + i;
            try {
                final Object builder = PROPERTIES_MAPPER.readPropertiesAs(
                        properties,
                        schema.withPrefix(sinkKey),
                        builderClass);

                @SuppressWarnings("unchecked") final com.arpnetworking.metrics.Sink sink =
                        (com.arpnetworking.metrics.Sink) builderClass.getMethod("build").invoke(builder);
                sinks.put(sinkKey, sink);
            } catch (final Exception e) {
                throw new RuntimeException(String.format("Unable to create sink %s from %s", builderClass, sinkKey), e);
            }
        }
        return sinks;
    }

    @Override
    protected void configureServlets() {
        bind(MetricReporterService.class).in(Singleton.class);

        final Map<String, Tagger> taggers = loadTaggers(parseTaggerBuilders(properties), properties);
        for (final Map.Entry<String, Tagger> entry : taggers.entrySet()) {
            bind(Tagger.class).annotatedWith(Names.named(entry.getKey())).toInstance(entry.getValue());
        }

        final Map<String, Sink> sinks = loadSinks(parseSinkBuilders(properties.getProperty(SINKS_KEY)), properties);
        for (final Map.Entry<String, Sink> entry : sinks.entrySet()) {
            bind(Sink.class).annotatedWith(Names.named(entry.getKey())).toInstance(entry.getValue());
        }
        final MetricsFactory metricsFactory = new TsdMetricsFactory.Builder()
                .setClusterName(properties.getProperty(CLUSTER_TAG_KEY))
                .setServiceName(properties.getProperty(SERVICE_TAG_KEY))
                .setSinks(new ArrayList<>(sinks.values()))
                .build();
        bind(MetricsFactory.class).toInstance(metricsFactory);

        if (!Strings.isNullOrEmpty(properties.getProperty(JVM_PERIOD_KEY))) {
            final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        final Thread t = new Thread(r, "JvmMetricsPoller");
                        t.setDaemon(true);
                        return t;
                    });

            final Duration interval = Duration.parse(properties.getProperty(JVM_PERIOD_KEY));
            executor.scheduleAtFixedRate(
                    new JvmMetricsRunnable.Builder()
                            .setMetricsFactory(metricsFactory)
                            .build(),
                    interval.toMillis(),
                    interval.toMillis(),
                    TimeUnit.MILLISECONDS);

            Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdown));
        }

        bind(MonitorFilter.class).in(Scopes.SINGLETON);
        filter("/*").through(MonitorFilter.class);

        final KairosMetricReporterListProvider reporterProvider = new KairosMetricReporterListProvider();
        bind(KairosMetricReporterListProvider.class).toInstance(reporterProvider);

        requestStaticInjection(ThreadReporter.class);

        bindListener(Matchers.any(), reporterProvider);
    }

    @Provides
    @javax.inject.Singleton
    public PeriodicMetrics getPeriodicMetrics(
            final MetricsFactory metricsFactory) {
        final TsdPeriodicMetrics periodicMetrics = new TsdPeriodicMetrics.Builder()
                .setMetricsFactory(metricsFactory)
                .build();
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                r -> {
                    final Thread t = new Thread(r, "PeriodicMetricsCloser");
                    t.setDaemon(true);
                    return t;
                });

        final Duration interval = Duration.parse(properties.getProperty(PERIOD_KEY));
        executor.scheduleAtFixedRate(periodicMetrics, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdown));

        return periodicMetrics;
    }
}