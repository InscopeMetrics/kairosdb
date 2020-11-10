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

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TagTagger}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class TagTaggerTest {

    @Mock
    private MetricsFactory metricsFactory;
    @Mock
    private Metrics metrics;
    @Mock
    private Supplier<String> metricNameSupplier;
    @Mock
    private Supplier<SetMultimap<String, String>> tagsSupplier;
    @Mock
    private BiConsumer<String, String> tagConsumer;

    public TagTaggerTest() {
        final SetMultimap<String, String> tags = HashMultimap.create();
        tags.put("foo", "bar");
        tags.put("123", "abc");
        MockitoAnnotations.initMocks(this);
        when(metricsFactory.create()).thenReturn(metrics);
        when(metricNameSupplier.get()).thenReturn("hows/my/metric/name");
        when(tagsSupplier.get()).thenReturn(tags);
    }

    @Test(expected = RuntimeException.class)
    public void testBuilderDuplicateMappedTargets() {
        new TagTagger.Builder()
                .setMappedTags(ImmutableMap.of(
                        "abc", "123",
                        "def", "123"))
                .build();
    }

    @Test(expected = RuntimeException.class)
    public void testBuilderDuplicateMapAndUnmappedTargets() {
        new TagTagger.Builder()
                .setTags(ImmutableSet.of("123"))
                .setMappedTags(ImmutableMap.of("abc", "123"))
                .build();
    }

    @Test
    public void testThreadReporter() {
        ThreadReporter.initialize(metricsFactory);
        final TagTagger tagger = new TagTagger.Builder()
                .setTags(ImmutableSet.of("foo", "123"))
                .build();
        tagger.applyTagsToThreadReporter(metricNameSupplier, tagsSupplier);
        ThreadReporter.close();

        verify(tagsSupplier).get();
        verifyZeroInteractions(metricNameSupplier);
        verify(metrics).addAnnotation("foo", "bar");
        verify(metrics).addAnnotation("123", "abc");
    }

    @Test
    public void testApplyTag() {
        final TagTagger tagger = new TagTagger.Builder()
                .setTags(ImmutableSet.of("123"))
                .build();
        tagger.applyTags(tagsSupplier, tagConsumer);
        verify(tagsSupplier).get();
        verify(tagConsumer).accept("123", "abc");
    }

    @Test
    public void testApplyMappedTag() {
        final TagTagger tagger = new TagTagger.Builder()
                .setMappedTags(ImmutableMap.of("123", "456"))
                .build();
        tagger.applyTags(tagsSupplier, tagConsumer);
        verify(tagsSupplier).get();
        verify(tagConsumer).accept("456", "abc");
    }

    @Test
    public void testApplyBoth() {
        final TagTagger tagger = new TagTagger.Builder()
                .setMappedTags(ImmutableMap.of("123", "456"))
                .setTags(ImmutableSet.of("foo"))
                .build();
        tagger.applyTags(tagsSupplier, tagConsumer);
        verify(tagsSupplier).get();
        verify(tagConsumer).accept("456", "abc");
        verify(tagConsumer).accept("foo", "bar");
    }
}
