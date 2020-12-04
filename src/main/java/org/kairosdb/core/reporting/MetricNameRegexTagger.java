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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;

import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Matches the metric name as a tag and applies an associated match as the
 * tag value. Only the first matching expression applies its value. An optional
 * default value may be applied if no expression matches. Otherwise, no tag
 * would be applied.
 *
 * This tagger only uses the information from the provided metric name supplier.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class MetricNameRegexTagger implements Tagger {

    private final ImmutableList<Rule> rules;

    private MetricNameRegexTagger(final Builder builder) {
        rules = builder.rules;
    }

    @Override
    public void applyTags(
            final BiConsumer<String, String> tagConsumer,
            final Supplier<String> metricNameSupplier,
            final Supplier<SetMultimap<String, String>> tags) {
        @Nullable final String metricName = metricNameSupplier.get();
        if (!Strings.isNullOrEmpty(metricName)) {
            for (final Rule rule : rules) {
                if (rule.apply(metricName, tagConsumer)) {
                    break;
                }
            }
        }
    }

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for {@link MetricNameRegexTagger}.
     */
    public static final class Builder implements com.arpnetworking.commons.builder.Builder<MetricNameRegexTagger> {

        private ImmutableList<Rule> rules = ImmutableList.of();

        @Override
        public MetricNameRegexTagger build() {
            return new MetricNameRegexTagger(this);
        }

        /**
         * Set list of pattern and replacement. Optional. Default is empty list.
         *
         * @param value list of pattern and replacement
         * @return this {@link Builder}
         */
        public Builder setRules(final ImmutableList<Rule> value) {
            rules = value;
            return this;
        }
    }

    public static final class Rule {
        private final Pattern pattern;
        private final String tagName;
        private final String tagValue;

        Rule(final Builder builder) {
            pattern = Pattern.compile(builder.pattern);
            tagName = builder.tagName;
            tagValue = builder.tagValue;
        }

        public boolean apply(final String value, final BiConsumer<String, String> tagConsumer) {
            final Matcher matcher = pattern.matcher(value);
            if (matcher.matches()) {
                final String replacedTagName = matcher.replaceAll(tagName);
                final String replaceTagValue = matcher.replaceAll(tagValue);
                tagConsumer.accept(replacedTagName, replaceTagValue);
                return true;
            }
            return false;
        }

        /**
         * {@link com.arpnetworking.commons.builder.Builder} implementation for {@link Rule}.
         */
        public static final class Builder implements com.arpnetworking.commons.builder.Builder<Rule> {

            private String pattern;
            private String tagName;
            private String tagValue;

            @Override
            public Rule build() {
                return new Rule(this);
            }

            /**
             * Set pattern to match. Required. Cannot be null. Must be a valid
             * Java pattern.
             *
             * @param value pattern to match
             * @return this {@link TagTagger.Builder}
             */
            public Builder setPattern(final String value) {
                pattern = value;
                return this;
            }

            /**
             * Set replacement tag name. Required. Cannot be null.
             *
             * @param value replacement tag name
             * @return this {@link TagTagger.Builder}
             */
            public Builder setTagName(final String value) {
                tagName = value;
                return this;
            }

            /**
             * Set replacement tag value. Required. Cannot be null.
             *
             * @param value replacement tag value
             * @return this {@link TagTagger.Builder}
             */
            public Builder setTagValue(final String value) {
                tagValue = value;
                return this;
            }
        }
    }
}
