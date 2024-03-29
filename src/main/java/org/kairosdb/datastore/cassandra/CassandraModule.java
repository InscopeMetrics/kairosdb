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
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import javax.inject.Named;

public class CassandraModule extends AbstractModule {
    public static final Logger logger = LoggerFactory.getLogger(CassandraModule.class);

    public static final String CASSANDRA_AUTH_MAP = "cassandra.auth.map";
    public static final String AUTH_PREFIX = "kairosdb.datastore.cassandra.auth.";

    private final Map<String, String> m_authMap = new HashMap<>();

    public CassandraModule(final KairosRootConfig props) {
        for (final String key : props) {
            if (key.startsWith(AUTH_PREFIX)) {
                final String consumerKey = key.substring(AUTH_PREFIX.length());
                final String consumerToken = props.getProperty(key);

                m_authMap.put(consumerKey, consumerToken);
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

        bind(new TypeLiteral<Map<String, String>>() {
        }).annotatedWith(Names.named(CASSANDRA_AUTH_MAP))
                .toInstance(m_authMap);


        install(new FactoryModuleBuilder().build(BatchHandlerFactory.class));

        install(new FactoryModuleBuilder().build(DeleteBatchHandlerFactory.class));

        install(new FactoryModuleBuilder().build(CQLBatchFactory.class));
        install(new FactoryModuleBuilder().build(CQLFilteredRowKeyIteratorFactory.class));
    }


    /**
     * Bind classes that are specific to the cluster connection
     *
     * @param binder
     * @param config
     */
    private void bindCassandraClient(final Binder binder, final ClusterConfiguration config) {
        binder.bind(ClusterConfiguration.class).toInstance(config);
        binder.bind(CassandraClient.class).to(CassandraClientImpl.class);
        binder.bindConstant().annotatedWith(Names.named("request_retry_count")).to(config.getRequestRetryCount());
        binder.bindConstant().annotatedWith(Names.named("cluster_name")).to(config.getClusterName());
        binder.bind(KairosRetryPolicy.class);
    }

    private ClusterConnection m_writeCluster;
    private ClusterConnection m_metaCluster;

    private void createClients(CassandraConfiguration configuration, Injector injector) {
        if (m_metaCluster != null)
            return;

        ClusterConfiguration writeConfig = configuration.getWriteCluster();
        ClusterConfiguration metaConfig = configuration.getMetaCluster();

        Injector writeInjector = injector.createChildInjector(binder -> bindCassandraClient(binder, writeConfig));

        CassandraClient writeClient = writeInjector.getInstance(CassandraClient.class);

        if (writeConfig == metaConfig) //No separate meta cluster configuration
        {
            m_metaCluster = m_writeCluster = new ClusterConnection(writeClient, EnumSet.of(
                    ClusterConnection.Type.WRITE, ClusterConnection.Type.META));
        } else {
            m_writeCluster = new ClusterConnection(writeClient, EnumSet.of(
                    ClusterConnection.Type.WRITE));

            Injector metaInjector = injector.createChildInjector(binder -> bindCassandraClient(binder, metaConfig));

            CassandraClient metaClient = metaInjector.getInstance(CassandraClient.class);

            m_metaCluster = new ClusterConnection(metaClient, EnumSet.of(
                    ClusterConnection.Type.META));
        }
    }

    @Provides
    @Singleton
    @Named("write_cluster")
    ClusterConnection getWriteCluster(CassandraConfiguration configuration, Injector injector) {
        try {
            createClients(configuration, injector);
            return m_writeCluster;
        } catch (Exception e) {
            logger.error("Error building write cluster", e);
            throw e;
        }


    }

    @Provides
    @Singleton
    @Named("meta_cluster")
    ClusterConnection getMetaCluster(final CassandraConfiguration configuration, final Injector injector)
            throws Exception {
        try {
            createClients(configuration, injector);
            return m_metaCluster;
        } catch (Exception e) {
            logger.error("Error building meta cluster", e);
            throw e;
        }
    }

    @Provides
    @Singleton
    List<ClusterConnection> getReadClusters(final CassandraConfiguration configuration, final Injector injector) {
        final ImmutableList.Builder<ClusterConnection> clusters = new ImmutableList.Builder<>();

        try {
            for (ClusterConfiguration clusterConfiguration : configuration.getReadClusters()) {
                final Injector readInjector = injector.createChildInjector(binder -> bindCassandraClient(binder, clusterConfiguration));

                final CassandraClient client = readInjector.getInstance(CassandraClient.class);

                clusters.add(new ClusterConnection(client, EnumSet.of(ClusterConnection.Type.READ)));
            }
        } catch (Exception e) {
            logger.error("Error building read cluster", e);
            throw e;
        }

        return clusters.build();
    }

    @Provides
    @Singleton
    LoadBalancingPolicy getLoadBalancingPolicy(@Named("write_cluster") final ClusterConnection connection) {
        return connection.getLoadBalancingPolicy();
    }

    @Provides
    @Singleton
    ConsistencyLevel getWriteConsistencyLevel(final CassandraConfiguration configuration) {
        return configuration.getWriteCluster().getWriteConsistencyLevel();
    }

    /*@Provides
    @Singleton
    Session getCassandraSession(final Schema schema) {
        return schema.getSession();
    }*/


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

    public interface CQLFilteredRowKeyIteratorFactory {
        CQLFilteredRowKeyIterator create(ClusterConnection cluster,
                                         String metricName,
                                         @Assisted("startTime") long startTime,
                                         @Assisted("endTime") long endTime,
                                         SetMultimap<String, String> filterTags) throws DatastoreException;
    }


}
