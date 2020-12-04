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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Applies all nested taggers in the order specified.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class MultiTagger implements Tagger {

    private final ImmutableList<Tagger> taggers;

    private MultiTagger(final Builder builder) {
        taggers = builder.taggers;
    }

    @Override
    public void applyTags(
            final BiConsumer<String, String> consumer,
            final Supplier<String> metricName,
            final Supplier<SetMultimap<String, String>> tags) {
        for (final Tagger tagger : taggers) {
            tagger.applyTags(consumer, metricName, tags);
        }
    }

    ImmutableList<Tagger> getTaggers() {
        return taggers;
    }

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for {@link MultiTagger}.
     */
    public static final class Builder implements com.arpnetworking.commons.builder.Builder<MultiTagger> {

        private ImmutableList<Tagger> taggers;

        @Override
        public MultiTagger build() {
            return new MultiTagger(this);
        }

        /**
         * Set nested taggers. Cannot be null. Default is empty.
         *
         * @param value nested taggers
         * @return this {@link TagTagger.Builder}
         */
        public Builder setTaggers(final ImmutableList<Tagger> value) {
            taggers = value;
            return this;
        }
    }
}
