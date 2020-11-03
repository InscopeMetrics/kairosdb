/*
 * Copyright 2020 Dropbox
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kairosdb.core.properties;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.reporting.MetricReporterService;
import org.kairosdb.datastore.cassandra.CassandraConfiguration;

/**
 * Ensure that nothing in the kairosdb.properties file would result in
 * unexpected behavior. Throw a KairosDBException upon startup if so.
 * <p>
 * Field injection is not ideal, but you can not provide defaults with
 * constructor injection.
 *
 * @author Max Wittek (https://github.com/wittekm)
 */
public class PropertiesValidator {
    @Inject(optional = true)
    @Named(CassandraConfiguration.ALIGN_DATAPOINT_TTL_WITH_TIMESTAMP)
    private boolean m_alignDatapointTtlWithTimestamp =
            CassandraConfiguration.DEFAULT_ALIGN_DATAPOINT_TTL_WITH_TIMESTAMP;

    @Inject(optional = true)
    @Named(MetricReporterService.REPORTER_TTL)
    private int m_reporterTtl = 0;

    @Inject(optional = true)
    @Named(CassandraConfiguration.DATAPOINT_TTL)
    private int m_cassandraDatapointTtl = CassandraConfiguration.DEFAULT_DATAPOINT_TTL;

    @Inject(optional = true)
    @Named(CassandraConfiguration.FORCE_DEFAULT_DATAPOINT_TTL)
    private boolean m_forceDefaultDatapointTtl = CassandraConfiguration.DEFAULT_FORCE_DEFAULT_DATAPOINT_TTL;

    public void validate() throws KairosDBException {
        validateSelfReportingTtlWorksWithAlignment();
        validateForceDefaultTtl();
    }

    /**
     * If {@code align_datapoint_ttl} is enabled, then the reporting ttl
     * must be set to a positive value or else all self-reported metrics
     * will be dropped.
     *
     * @throws KairosDBException if the configuration is invalid
     */
    private void validateSelfReportingTtlWorksWithAlignment() throws KairosDBException {
        if (m_alignDatapointTtlWithTimestamp && m_reporterTtl <= 0) {
            throw new KairosDBException(String.format(
                    "Since %s is turned on, you must set %s to a positive value.",
                    CassandraConfiguration.ALIGN_DATAPOINT_TTL_WITH_TIMESTAMP,
                    MetricReporterService.REPORTER_TTL));
        }
    }

    /**
     * If {@code kairosdb.datastore.cassandra.force_default_datapoint_ttl}
     * is enabled, then the Cassandra data point ttl must be set to a
     * positive value or else all metrics will be dropped.
     *
     * @throws KairosDBException if the configuration is invalid
     */
    private void validateForceDefaultTtl() throws KairosDBException {
        if (m_forceDefaultDatapointTtl && m_cassandraDatapointTtl <= 0) {
            throw new KairosDBException(String.format(
                    "Since %s is turned on, you must set %s to a positive value.",
                    CassandraConfiguration.FORCE_DEFAULT_DATAPOINT_TTL,
                    CassandraConfiguration.DATAPOINT_TTL));
        }
    }
}
