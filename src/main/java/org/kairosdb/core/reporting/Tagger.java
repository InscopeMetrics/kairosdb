/*
 * Copyright 2020 Dropbox
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kairosdb.core.reporting;

import com.arpnetworking.metrics.Metrics;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.util.function.Supplier;

public interface Tagger {

    /**
     * Applies the tags from the available information to the
     * thread local {@link ThreadReporter}.
     *
     * @param metricName supplies the metric name if available
     * @param tags supplies the tags if available
     */
    void applyTagsToThreadReporter(
            Supplier<String> metricName,
            Supplier<SetMultimap<String, String>> tags);

    /**
     * Applies the tags from the available information to the
     * supplied {@link Metrics} instance.
     *
     * @param metrics the {@link Metrics} instance to add tags to
     * @param metricName supplies the metric name if available
     * @param tags supplies the tags if available
     */
    void applyTagsToMetrics(
            Metrics metrics,
            Supplier<String> metricName,
            Supplier<SetMultimap<String, String>> tags);

    /**
     * Returns the tags from the available information.
     *
     * @param metricName supplies the metric name if available
     * @param tags supplies the tags if available
     * @return the extracted tags
     */
    Multimap<String, String> createTags(
            Supplier<String> metricName,
            Supplier<SetMultimap<String, String>> tags);
}