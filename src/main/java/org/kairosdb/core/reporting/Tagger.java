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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "class")
public interface Tagger {

    /**
     * Applies the tags from the available information to the
     * {@link BiConsumer}.
     *
     * @param consumer accepts each tag key-value pair
     * @param metricName supplies the metric name if available
     * @param tags supplies the tags if available
     */
    void applyTags(
            BiConsumer<String, String> consumer,
            Supplier<String> metricName,
            Supplier<SetMultimap<String, String>> tags);

    /**
     * Applies the tags from the available information to the
     * thread local {@link ThreadReporter}.
     *
     * @param metricName supplies the metric name if available
     * @param tags supplies the tags if available
     */
    default void applyTagsToThreadReporter(
            Supplier<String> metricName,
            Supplier<SetMultimap<String, String>> tags) {
        applyTags(ThreadReporter::addTag, metricName, tags);
    }

    /**
     * Applies the tags from the available information to the
     * supplied {@link Metrics} instance.
     *
     * @param metrics the {@link Metrics} instance to add tags to
     * @param metricName supplies the metric name if available
     * @param tags supplies the tags if available
     */
    default void applyTagsToMetrics(
            Metrics metrics,
            Supplier<String> metricName,
            Supplier<SetMultimap<String, String>> tags) {
        applyTags(metrics::addAnnotation, metricName, tags);
    }

    /**
     * Returns the tags from the available information.
     *
     * @param metricName supplies the metric name if available
     * @param tags supplies the tags if available
     * @return the extracted tags
     */
    default Multimap<String, String> createTags(
            Supplier<String> metricName,
            Supplier<SetMultimap<String, String>> tags) {
        final Multimap<String, String> result = HashMultimap.create();
        applyTags(result::put, metricName, tags);
        return result;
    }
}
