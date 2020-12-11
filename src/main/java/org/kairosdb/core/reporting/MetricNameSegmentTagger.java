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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.SetMultimap;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Applies specified number of segment prefix from the metric name as a tag. By
 * default uses the entire metric name; however, can be configured with a split
 * string and segment count to promote a prefix.
 *
 * e.g. "my/foo/bar/metric", split="/", segments=2 => "my/foo"
 *
 * This tagger only uses the information from the provided metric name supplier.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class MetricNameSegmentTagger implements Tagger {

    private final String splitter;
    private final int segments;

    private MetricNameSegmentTagger(final Builder builder) {
        splitter = builder.splitter;
        segments = builder.segments;
    }

    @Override
    public void applyTags(
            final BiConsumer<String, String> tagConsumer,
            final Supplier<String> metricNameSupplier,
            final Supplier<SetMultimap<String, String>> tags) {
        @Nullable final String metricName = metricNameSupplier.get();
        if (!Strings.isNullOrEmpty(metricName)) {
            final String metricNameTagValue;
            if (segments > 0) {
                metricNameTagValue = Joiner.on(splitter).join(
                        Arrays.copyOfRange(
                                metricName.split(splitter),
                                0,
                                segments));
            } else {
                metricNameTagValue = metricName;
            }
            tagConsumer.accept("metricName", metricNameTagValue);
        }
    }

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for {@link MetricNameSegmentTagger}.
     */
    public static final class Builder implements com.arpnetworking.commons.builder.Builder<MetricNameSegmentTagger> {

        private String splitter = "/";
        private Integer segments = 0;

        @Override
        public MetricNameSegmentTagger build() {
            return new MetricNameSegmentTagger(this);
        }

        /**
         * Set split string. Optional. Default is forward slash.
         *
         * @param value split string for counting segments
         * @return this {@link TagTagger.Builder}
         */
        public Builder setSplitter(final String value) {
            splitter = value;
            return this;
        }

        /**
         * Set segments. Optional. Default is 0. Any value equal to or less than
         * zero promotes all segments (e.g. the entire metric name).
         *
         * @param value segments to tag with
         * @return this {@link TagTagger.Builder}
         */
        public Builder setSegments(final Integer value) {
            segments = value;
            return this;
        }
    }
}
