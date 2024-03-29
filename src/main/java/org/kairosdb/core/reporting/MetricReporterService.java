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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.quartz.TriggerBuilder.newTrigger;

public class MetricReporterService implements KairosDBJob {
    public static final Logger logger = LoggerFactory.getLogger(MetricReporterService.class);

    public static final String SCHEDULE_PROPERTY = "kairosdb.reporter.schedule";
    public static final String REPORTER_TTL = "kairosdb.reporter.ttl";

    private final String m_schedule;
    private final int m_ttl;
    private final Publisher<DataPointEvent> m_publisher;
    private final KairosMetricReporterListProvider m_reporterProvider;

    @Inject
    public MetricReporterService(final FilterEventBus eventBus,
                                 final KairosMetricReporterListProvider reporterProvider,
                                 @Named(SCHEDULE_PROPERTY) final String schedule,
                                 @Named(REPORTER_TTL) final int ttl) {
        m_reporterProvider = reporterProvider;
        m_schedule = schedule;
        m_ttl = ttl;

        m_publisher = eventBus.createPublisher(DataPointEvent.class);
    }

    @Override
    public Trigger getTrigger() {
        return (newTrigger()
                .withIdentity(this.getClass().getSimpleName())
                .withSchedule(CronScheduleBuilder.cronSchedule(m_schedule))
                .build());
    }

    @Override
    public void interrupt() {
    }

    @Override
    public void execute(final JobExecutionContext jobExecutionContext) {
        logger.debug("Reporting metrics");
        final long timestamp = System.currentTimeMillis();
        try {
            for (final KairosMetricReporter reporter : m_reporterProvider.get()) {
                final List<DataPointSet> dpList = reporter.getMetrics(timestamp);
                for (final DataPointSet dataPointSet : dpList) {
                    for (final DataPoint dataPoint : dataPointSet.getDataPoints()) {
                        m_publisher.post(new DataPointEvent(dataPointSet.getName(),
                                dataPointSet.getTags(), dataPoint, m_ttl));
                    }
                }
            }
        } catch (final Throwable e) {
            // prevent the thread from dying
            logger.error("Reporter service error", e);
        }
    }
}
