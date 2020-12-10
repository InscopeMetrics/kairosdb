/*
 * Copyright 2020 Dropbox
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link MultiTagger}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class MultiTaggerTest {

    @Mock
    private Supplier<String> metricNameSupplier;
    @Mock
    private Supplier<SetMultimap<String, String>> tagsSupplier;
    @Mock
    private BiConsumer<String, String> tagConsumer;
    @Mock
    private Tagger taggerA;
    @Mock
    private Tagger taggerB;

    public MultiTaggerTest() {
        final SetMultimap<String, String> tags = HashMultimap.create();
        tags.put("foo", "bar");
        MockitoAnnotations.initMocks(this);
        when(tagsSupplier.get()).thenReturn(tags);
    }

    @Test
    public void test() {
        final Tagger tagger = new MultiTagger.Builder()
                .setTaggers(ImmutableList.of(taggerA, taggerB))
                .build();
        tagger.applyTags(tagConsumer, metricNameSupplier, tagsSupplier);
        verify(taggerA).applyTags(tagConsumer, metricNameSupplier, tagsSupplier);
        verify(taggerB).applyTags(tagConsumer, metricNameSupplier, tagsSupplier);
    }

    @Test
    public void testAppliedInOrder() {
        final Tagger tagger1 = new TagTagger.Builder()
                .setMappedTags(ImmutableMap.of("foo", "bar1"))
                .build();
        final Tagger tagger2 = new TagTagger.Builder()
                .setMappedTags(ImmutableMap.of("foo", "bar2"))
                .build();
        final Tagger tagger3 = new TagTagger.Builder()
                .setMappedTags(ImmutableMap.of("foo", "bar3"))
                .build();

        final Tagger tagger = new MultiTagger.Builder()
                .setTaggers(ImmutableList.of(tagger1, tagger2, tagger3))
                .build();

        final List<String> actual = Lists.newArrayList();
        tagger.applyTags((k, v) -> actual.add(k), metricNameSupplier, tagsSupplier);

        final List<String> expected = Lists.newArrayList("bar1", "bar2", "bar3");

        assertEquals(3, actual.size());
        assertEquals(expected, actual);
    }
}
