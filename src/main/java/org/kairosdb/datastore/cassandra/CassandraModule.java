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

package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import javax.inject.Named;

public class CassandraModule extends AbstractModule {
    public static final Logger logger = LoggerFactory.getLogger(CassandraModule.class);

    public static final String CASSANDRA_AUTH_MAP = "cassandra.auth.map";
    public static final String CASSANDRA_HECTOR_MAP = "cassandra.hector.map";
    public static final String AUTH_PREFIX = "kairosdb.datastore.cassandra.auth.";
    public static final String HECTOR_PREFIX = "kairosdb.datastore.cassandra.hector.";

    private final Map<String, String> m_authMap = new HashMap<>();
    private final Map<String, Object> m_hectorMap = new HashMap<>();

    public CassandraModule(final Properties props) {
        for (final Object key : props.keySet()) {
            final String strKey = (String) key;

            if (strKey.startsWith(AUTH_PREFIX)) {
                final String consumerKey = strKey.substring(AUTH_PREFIX.length());
                final String consumerToken = (String) props.get(key);

                m_authMap.put(consumerKey, consumerToken);
            } else if (strKey.startsWith(HECTOR_PREFIX)) {
                final String configKey = strKey.substring(HECTOR_PREFIX.length());
                m_hectorMap.put(configKey, props.get(key));
            }
        }
    }


    @Override
    protected void configure() {
        bind(Datastore.class).to(CassandraDatastore.class).in(Scopes.SINGLETON);
        bind(ServiceKeyStore.class).to(CassandraDatastore.class).in(Scopes.SINGLETON);
        bind(CassandraDatastore.class).in(Scopes.SINGLETON);
        bind(CleanRowKeyCache.class).in(Scopes.SINGLETON);
        bind(CassandraConfiguration.class).in(Scopes.SINGLETON);
        //bind(CassandraClient.class).to(CassandraClientImpl.class);
        bind(CassandraClientImpl.class).in(Scopes.SINGLETON);

        bind(new TypeLiteral<Map<String, String>>() {
        }).annotatedWith(Names.named(CASSANDRA_AUTH_MAP))
                .toInstance(m_authMap);

		/*bind(new TypeLiteral<Map<String, Object>>(){}).annotatedWith(Names.named(CASSANDRA_HECTOR_MAP))
				.toInstance(m_hectorMap);*/


        install(new FactoryModuleBuilder().build(BatchHandlerFactory.class));

        install(new FactoryModuleBuilder().build(DeleteBatchHandlerFactory.class));

        install(new FactoryModuleBuilder().build(CQLBatchFactory.class));
    }

    @Provides
    @Named("keyspace")
    String getKeyspace(final CassandraConfiguration configuration) {
        return configuration.getKeyspaceName();
    }

    @Provides
    @Singleton
    CassandraClient getCassandraClient(final CassandraConfiguration configuration, final Injector injector) {
        try {
            final CassandraClientImpl client = injector.getInstance(CassandraClientImpl.class);
            client.init();
            return client;
        } catch (final Exception e) {
            logger.error("Unable to setup cassandra connection to cluster", e);
            throw e;
        }
    }

    @Provides
    @Singleton
    Schema getCassandraSchema(final CassandraClient cassandraClient, final CassandraConfiguration configuration) {
        try {
            return new Schema(cassandraClient, configuration.isCreateSchema());
        } catch (final Exception e) {
            logger.error("Unable to setup cassandra schema", e);
            throw e;
        }
    }

    @Provides
    @Singleton
    LoadBalancingPolicy getLoadBalancingPolicy(final CassandraClient cassandraClient) {
        return cassandraClient.getWriteLoadBalancingPolicy();
    }

    @Provides
    @Singleton
    ConsistencyLevel getWriteConsistencyLevel(final CassandraConfiguration configuration) {
        return configuration.getDataWriteLevel();
    }

    @Provides
    @Singleton
    Session getCassandraSession(final Schema schema) {
        return schema.getSession();
    }


    @Provides
    @Singleton
    DataCache<DataPointsRowKey> getRowKeyCache(
            final CassandraConfiguration configuration,
            final PeriodicMetrics periodicMetrics) {
        return new DataCache<>("row_key", configuration.getRowKeyCacheSize(), periodicMetrics);
    }

    @Provides
    @Singleton
    DataCache<String> getMetricNameCache(
            final CassandraConfiguration configuration,
            final PeriodicMetrics periodicMetrics) {
        return new DataCache<>("metric_name", configuration.getStringCacheSize(), periodicMetrics);
    }

    public interface BatchHandlerFactory {
        BatchHandler create(List<DataPointEvent> events, EventCompletionCallBack callBack,
                            boolean fullBatch);
    }

    public interface DeleteBatchHandlerFactory {
        DeleteBatchHandler create(String metricName, SortedMap<String, String> tags,
                                  List<DataPoint> dataPoints, EventCompletionCallBack callBack);
    }

    public interface CQLBatchFactory {
        CQLBatch create();
    }


}
