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

package org.kairosdb.datastore.remote;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.quartz.TriggerBuilder.newTrigger;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 6/3/13
 * Time: 4:22 PM
 * To change this template use File | Settings | File Templates.
 */
@DisallowConcurrentExecution
@SuppressWarnings("deprecation")
public class RemoteSendJob implements KairosDBJob {
    public static final Logger logger = LoggerFactory.getLogger(RemoteSendJob.class);
    private static final String SCHEDULE = "kairosdb.datastore.remote.schedule";
    private static final String DELAY = "kairosdb.datastore.remote.random_delay";

    private final String m_schedule;
    private final int m_delay;
    private final Random m_rand;
    private final RemoteDatastore m_datastore;
    private Exception m_currentException;

    @Inject
    public RemoteSendJob(@Named(SCHEDULE) final String schedule,
                         @Named(DELAY) final int delay, final RemoteDatastore datastore) {
        m_schedule = schedule;
        m_delay = delay;
        m_datastore = datastore;

        m_rand = new Random(System.currentTimeMillis());
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
    public void execute(final JobExecutionContext jobExecutionContext) throws JobExecutionException {
        if (m_delay != 0) {
            final int delay = m_rand.nextInt(m_delay);

            try {
                Thread.sleep(delay * 1000L);
            } catch (final InterruptedException e) {
                logger.warn("Sleep delay interrupted", e);
            }
        }

        try {
            logger.debug("Sending remote data");
            m_datastore.sendData();
            m_currentException = null;
            logger.debug("Finished sending remote data");
        } catch (final Exception e) {
            logger.error("Unable to send remote data", e);
            m_currentException = e;
            throw new JobExecutionException("Unable to send remote data: " + e.getMessage());
        }
    }

    Exception getCurrentException() {
        return m_currentException;
    }
}
