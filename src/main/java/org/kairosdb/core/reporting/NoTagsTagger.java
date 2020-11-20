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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.util.function.Supplier;

/**
 * Does not apply any tags.
 *
 * This tagger does not use any of the information from the provided metric name
 * or tags suppliers.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class NoTagsTagger implements Tagger {

    private static final NoTagsTagger INSTANCE = new NoTagsTagger();

    private NoTagsTagger() { }

    @Override
    public void applyTagsToThreadReporter(
            final Supplier<String> metricName,
            final Supplier<SetMultimap<String, String>> tags) {
        // Intentionally empty
    }

    @Override
    public void applyTagsToMetrics(
            final Metrics metrics,
            final Supplier<String> metricName,
            final Supplier<SetMultimap<String, String>> tags) {
        // Intentionally empty
    }

    @Override
    public Multimap<String, String> createTags(
            Supplier<String> metricName,
            Supplier<SetMultimap<String, String>> tags) {
        return ImmutableMultimap.of();
    }

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for {@link NoTagsTagger}.
     */
    public static final class Builder implements com.arpnetworking.commons.builder.Builder<NoTagsTagger> {

        @Override
        public NoTagsTagger build() {
            return INSTANCE;
        }
    }
}
