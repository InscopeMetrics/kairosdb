package org.kairosdb.core.http.rest;


import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.kairosdb.core.GuiceKairosDataPointFactory;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.KairosFeatureProcessor;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.QueryPluginFactory;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.WebServer;
import org.kairosdb.core.http.WebServletModule;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;
import org.kairosdb.core.processingstage.FeatureProcessingFactory;
import org.kairosdb.core.processingstage.FeatureProcessor;
import org.kairosdb.eventbus.EventBusConfiguration;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;
import org.kairosdb.testing.Client;
import org.mockito.Mockito;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public abstract class ResourceBase {
    private static final FilterEventBus eventBus = new FilterEventBus(new EventBusConfiguration(new KairosRootConfig()));
    static QueryQueuingManager queuingManager;
    static Client client;
    static TestDatastore datastore;
    static MetricsResource resource;
    private static WebServer server;

    @BeforeClass
    public static void startup() throws Exception {
        //This sends jersey java util logging to logback
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        final PeriodicMetrics periodicMetrics = Mockito.mock(PeriodicMetrics.class);
        datastore = new TestDatastore();
        queuingManager = new QueryQueuingManager(periodicMetrics, 3);

        final Injector injector = Guice.createInjector(new WebServletModule(new KairosRootConfig()), new AbstractModule() {
            @Override
            protected void configure() {
                bind(FilterEventBus.class).toInstance(eventBus);
                //Need to register an exception handler
                bindListener(Matchers.any(), new TypeListener() {
                    public <I> void hear(final TypeLiteral<I> typeLiteral, final TypeEncounter<I> typeEncounter) {
                        typeEncounter.register((InjectionListener<I>) i -> eventBus.register(i));
                    }
                });

                final MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
                final Metrics metrics = Mockito.mock(Metrics.class);
                Mockito.doReturn(metrics).when(metricsFactory).create();

                bind(String.class).annotatedWith(Names.named(WebServer.JETTY_ADDRESS_PROPERTY)).toInstance("0.0.0.0");
                bind(Integer.class).annotatedWith(Names.named(WebServer.JETTY_PORT_PROPERTY)).toInstance(9001);
                bind(String.class).annotatedWith(Names.named(WebServer.JETTY_WEB_ROOT_PROPERTY)).toInstance("bogus");
                bind(Boolean.class).annotatedWith(Names.named(WebServer.JETTY_SHOW_STACKTRACE)).toInstance(Boolean.FALSE);
                bind(Datastore.class).toInstance(datastore);
                bind(ServiceKeyStore.class).toInstance(datastore);
                bind(KairosDatastore.class).in(Singleton.class);
                bind(FeaturesResource.class).in(Singleton.class);
                bind(FeatureProcessor.class).to(KairosFeatureProcessor.class);
                bind(new TypeLiteral<FeatureProcessingFactory<Aggregator>>() {
                }).to(TestAggregatorFactory.class);
                bind(new TypeLiteral<FeatureProcessingFactory<GroupBy>>() {
                }).to(TestGroupByFactory.class);
                bind(QueryParser.class).in(Singleton.class);
                bind(QueryQueuingManager.class).toInstance(queuingManager);
                bindConstant().annotatedWith(Names.named("HOSTNAME")).to("HOST");
                bindConstant().annotatedWith(Names.named("kairosdb.datastore.concurrentQueryThreads")).to(1);
                bindConstant().annotatedWith(Names.named("kairosdb.query_cache.keep_cache_files")).to(false);
                bind(KairosDataPointFactory.class).to(GuiceKairosDataPointFactory.class);
                bind(QueryPluginFactory.class).to(TestQueryPluginFactory.class);
                bind(String.class).annotatedWith(Names.named("kairosdb.server.type")).toInstance("ALL");
                bind(MetricsFactory.class).toInstance(metricsFactory);
                bind(PeriodicMetrics.class).toInstance(Mockito.mock(PeriodicMetrics.class));

                final KairosRootConfig props = new KairosRootConfig();
                String configFileName = "kairosdb.properties";
                final InputStream is = getClass().getClassLoader().getResourceAsStream(configFileName);
                try {
                    props.load(is, KairosRootConfig.ConfigFormat.fromFileName(configFileName));
                    is.close();
                } catch (final IOException e) {
                    e.printStackTrace();
                }

                //Names.bindProperties(binder(), props);
                bind(KairosRootConfig.class).toInstance(props);

                bind(DoubleDataPointFactory.class)
                        .to(DoubleDataPointFactoryImpl.class).in(Singleton.class);
                bind(DoubleDataPointFactoryImpl.class).in(Singleton.class);

                bind(LongDataPointFactory.class)
                        .to(LongDataPointFactoryImpl.class).in(Singleton.class);
                bind(LongDataPointFactoryImpl.class).in(Singleton.class);

                bind(LegacyDataPointFactory.class).in(Singleton.class);

                bind(QueryPreProcessorContainer.class).to(GuiceQueryPreProcessor.class).in(javax.inject.Singleton.class);
            }
        });
        KairosDatastore kairosDatastore = injector.getInstance(KairosDatastore.class);
        kairosDatastore.init();

        server = injector.getInstance(WebServer.class);
        server.start();

        client = new Client();
        resource = injector.getInstance(MetricsResource.class);
    }

    @AfterClass
    public static void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    public static class TestDatastore implements Datastore, ServiceKeyStore {
        private DatastoreException m_toThrow = null;
        private final Map<String, String> metadata = new TreeMap<>();

        TestDatastore()
        {
        }

        void throwException(final DatastoreException toThrow) {
            m_toThrow = toThrow;
        }

        @Override
        public void close()
        {
        }

        @Override
        public Iterable<String> getMetricNames(final String prefix) {
            return Arrays.asList("cpu", "memory", "disk", "network");
        }

        @Override
        public Iterable<String> getTagNames() {
            return Arrays.asList("server1", "server2", "server3");
        }

        @Override
        public Iterable<String> getTagValues() {
            return Arrays.asList("larry", "moe", "curly");
        }

        @Override
        public void queryDatabase(final DatastoreMetricQuery query, final QueryCallback queryCallback) throws DatastoreException {
            if (m_toThrow != null)
                throw m_toThrow;

            try {
                SortedMap<String, String> tags = new TreeMap<>();
                tags.put("server", "server1");

                QueryCallback.DataPointWriter dataPointWriter = queryCallback.startDataPointSet(LongDataPointFactoryImpl.DST_LONG, tags);
                dataPointWriter.addDataPoint(new LongDataPoint(1, 10));
                dataPointWriter.addDataPoint(new LongDataPoint(1, 20));
                dataPointWriter.addDataPoint(new LongDataPoint(2, 10));
                dataPointWriter.addDataPoint(new LongDataPoint(2, 5));
                dataPointWriter.addDataPoint(new LongDataPoint(3, 10));
                dataPointWriter.close();

                tags = new TreeMap<>();
                tags.put("server", "server2");

                dataPointWriter = queryCallback.startDataPointSet(DoubleDataPointFactoryImpl.DST_DOUBLE, tags);
                dataPointWriter.addDataPoint(new DoubleDataPoint(1, 10.1));
                dataPointWriter.addDataPoint(new DoubleDataPoint(1, 20.1));
                dataPointWriter.addDataPoint(new DoubleDataPoint(2, 10.1));
                dataPointWriter.addDataPoint(new DoubleDataPoint(2, 5.1));
                dataPointWriter.addDataPoint(new DoubleDataPoint(3, 10.1));

                dataPointWriter.close();
            } catch (final IOException e) {
                throw new DatastoreException(e);
            }
        }

        @Override
        public void deleteDataPoints(final DatastoreMetricQuery deleteQuery)  {
        }

        @Override
        public TagSet queryMetricTags(final DatastoreMetricQuery query)  {
            return null;
        }

        @Override
        public long queryCardinality(final DatastoreMetricQuery query) {
            return 0;
        }

        @Override
        public void setValue(final String service, final String serviceKey, final String key, final String value) throws DatastoreException {
            if (m_toThrow != null)
                throw m_toThrow;
            metadata.put(service + "/" + serviceKey + "/" + key, value);
        }

        @Override
        public ServiceKeyValue getValue(final String service, final String serviceKey, final String key) throws DatastoreException {
            if (m_toThrow != null)
                throw m_toThrow;
            return new ServiceKeyValue(metadata.get(service + "/" + serviceKey + "/" + key), new Date());
        }

        @Override
        public Iterable<String> listServiceKeys(final String service)
                throws DatastoreException {
            if (m_toThrow != null)
                throw m_toThrow;

            final Set<String> keys = new HashSet<>();
            for (final String key : metadata.keySet()) {
                if (key.startsWith(service)) {
                    keys.add(key.split("/")[1]);
                }
            }
            return keys;
        }

        @Override
        public Iterable<String> listKeys(final String service, final String serviceKey) throws DatastoreException {
            if (m_toThrow != null)
                throw m_toThrow;

            final List<String> keys = new ArrayList<>();
            for (final String key : metadata.keySet()) {
                if (key.startsWith(service + "/" + serviceKey)) {
                    keys.add(key.split("/")[2]);
                }
            }
            return keys;
        }

        @Override
        public Iterable<String> listKeys(final String service, final String serviceKey, final String keyStartsWith) throws DatastoreException {
            if (m_toThrow != null)
                throw m_toThrow;

            final List<String> keys = new ArrayList<>();
            for (final String key : metadata.keySet()) {
                if (key.startsWith(service + "/" + serviceKey + "/" + keyStartsWith)) {
                    keys.add(key.split("/")[2]);
                }
            }
            return keys;
        }

        @Override
        public void deleteKey(final String service, final String serviceKey, final String key)
                throws DatastoreException {
            if (m_toThrow != null)
                throw m_toThrow;

            metadata.remove(service + "/" + serviceKey + "/" + key);
        }

        @Override
        public Date getServiceKeyLastModifiedTime(final String service, final String serviceKey)
                 {
            return null;
        }
    }
}
