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

package org.kairosdb.core.reporting;

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;

import javax.annotation.Nullable;

/**
 * Thread local scoped metrics reporter. Rewritten to record metrics against a
 * {@link Metrics} instance.
 *
 * Original version by bhawkins.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class ThreadReporter {
    private static final ThreadLocal<Metrics> s_metrics = new ThreadLocal<>();

    private ThreadReporter() { }

    public static void initialize(final MetricsFactory metricsFactory) {
        s_metrics.set(metricsFactory.create());
    }

    public static void addTag(final String name, final String value) {
        @Nullable final Metrics metrics = s_metrics.get();
        if (metrics != null) {
            metrics.addAnnotation(name, value);
        }
    }

    public static void close() {
        @Nullable final Metrics metrics = s_metrics.get();
        if (metrics != null) {
            metrics.close();
            s_metrics.set(null);
        }
    }

    public static void addDataPoint(final String metric, final long value) {
        @Nullable final Metrics metrics = s_metrics.get();
        if (metrics != null) {
            metrics.setGauge(metric, value);
        }
    }
}
