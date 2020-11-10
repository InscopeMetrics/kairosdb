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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Applies the select tags in the context as a tag.
 *
 * This tagger only uses the information from the provided tags supplier.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class TagTagger implements Tagger {

    private static final Logger LOGGER = LoggerFactory.getLogger(TagTagger.class);

    private final ImmutableMap<String, String> tagMapping;

    private TagTagger(final Builder builder) {
        final ImmutableMap.Builder<String, String> tagMappingBuilder = ImmutableMap.builder();
        builder.tags.forEach(e -> tagMappingBuilder.put(e, e));
        tagMappingBuilder.putAll(builder.mappedTags.entrySet());
        tagMapping = tagMappingBuilder.build();
    }

    @Override
    public void applyTagsToThreadReporter(
            final Supplier<String> metricName,
            final Supplier<SetMultimap<String, String>> tags) {
        applyTags(tags, ThreadReporter::addTag);
    }

    ImmutableMap<String, String> getTagMapping() {
        return tagMapping;
    }

    void applyTags(
            final Supplier<SetMultimap<String, String>> tagsSupplier,
            final BiConsumer<String, String> tagConsumer) {
        @Nullable final SetMultimap<String, String> tags = tagsSupplier.get();
        if (tags != null) {
            for (final String tagName : tags.keySet()) {
                @Nullable final String tagTargetName = tagMapping.get(tagName);
                if (tagTargetName != null) {
                    for (final String tagValue : tags.get(tagName)) {
                        tagConsumer.accept(tagTargetName, tagValue);
                    }
                }
            }
        }
    }

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for {@link TagTagger}.
     */
    public static final class Builder implements com.arpnetworking.commons.builder.Builder<TagTagger> {

        private ImmutableSet<String> tags = ImmutableSet.of();
        private ImmutableMap<String, String> mappedTags = ImmutableMap.of();

        @Override
        public TagTagger build() {
            if (!validate()) {
                throw new RuntimeException("Invalid TagTagger");
            }
            return new TagTagger(this);
        }

        /**
         * Set tag names. Cannot be null. Default is empty.
         *
         * @param value tag names
         * @return this {@link Builder}
         */
        public Builder setTags(final ImmutableSet<String> value) {
            tags = value;
            return this;
        }

        /**
         * Set mapped tags. Cannot be null. Default is empty.
         *
         * @param value mapped tag
         * @return this {@link Builder}
         */
        public Builder setMappedTags(final ImmutableMap<String, String> value) {
            mappedTags = value;
            return this;
        }

        private boolean validate() {
            final Set<String> mappedTargetTags = Sets.newHashSet(mappedTags.values());

            // Assert that no two mapped tags target the same value
            if (mappedTargetTags.size() != mappedTags.size()) {
                LOGGER.warn(String.format(
                        "Invalid TagTagger; reason: Mapped tags target the same key name, mappedTags: %s",
                        mappedTags));
                return false;
            }

            // Assert that no mapped tags target is also a tag
            if (!Sets.intersection(mappedTargetTags, tags).isEmpty()) {
                LOGGER.warn(String.format(
                        "Invalid TagTagger; reason: Mapped tags overlap with (unmapped) tags, tags: %s, mappedTags: %s",
                        tags,
                        mappedTags));
                return false;
            }

            // NOTE: This does mean that a logically equivalent overlap would be
            // disallowed. For example:
            //
            // tags = {"a", "b"}
            // mappedTags = {"b": "b"}
            //
            // This is logically valid because the tag rule for "b" and the
            // mapped tag rule for "b" are equivalent! However, the current
            // check disallows this.

            return true;
        }
    }
}
