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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link MetricNameRegexTagger}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class MetricNameRegexTaggerTest {

    private final AutoCloseable mocks;
    @Mock
    private MetricsFactory metricsFactory;
    @Mock
    private Metrics metrics;
    @Mock
    private Supplier<String> metricNameSupplier;
    @Mock
    private Supplier<SetMultimap<String, String>> tagsSupplier;

    public MetricNameRegexTaggerTest() {
        final SetMultimap<String, String> tags = HashMultimap.create();
        tags.put("foo", "bar");
        tags.put("123", "abc");
        mocks = MockitoAnnotations.openMocks(this);
        when(metricsFactory.create()).thenReturn(metrics);
        when(metricNameSupplier.get()).thenReturn("hows/my/metric/name");
        when(tagsSupplier.get()).thenReturn(tags);
    }

    @After
    public void tearDown() throws Exception {
        mocks.close();
    }

    @Test
    public void testSingleRule() {
        final Tagger tagger = new MetricNameRegexTagger.Builder()
                .setRules(ImmutableList.of(
                        new MetricNameRegexTagger.Rule.Builder()
                                .setPattern("([^ ]*) (.*)")
                                .setTagName("name_$1")
                                .setTagValue("value_$2")
                                .build()))
                .build();

        Multimap<String, String> tags;

        when(metricNameSupplier.get()).thenReturn("foo bar");
        tags = tagger.createTags(metricNameSupplier, tagsSupplier);
        assertEquals(1, tags.size());
        assertEquals("value_bar", Iterables.getOnlyElement(tags.get("name_foo")));

        when(metricNameSupplier.get()).thenReturn("foobar");
        tags = tagger.createTags(metricNameSupplier, tagsSupplier);
        assertTrue(tags.isEmpty());
    }

    @Test
    public void testRollupExtraction() {
        final Tagger tagger = new MetricNameRegexTagger.Builder()
                .setRules(ImmutableList.of(
                        new MetricNameRegexTagger.Rule.Builder()
                                .setPattern("^.*_1h$")
                                .setTagName("rollup")
                                .setTagValue("hourly")
                                .build(),
                        new MetricNameRegexTagger.Rule.Builder()
                                .setPattern("^.*_1d$")
                                .setTagName("rollup")
                                .setTagValue("daily")
                                .build(),
                        new MetricNameRegexTagger.Rule.Builder()
                                .setPattern("^.*$")
                                .setTagName("rollup")
                                .setTagValue("none")
                                .build()))
                .build();

        Multimap<String, String> tags;

        when(metricNameSupplier.get()).thenReturn("my/metric/name_1h");
        tags = tagger.createTags(metricNameSupplier, tagsSupplier);
        assertEquals(1, tags.size());
        assertEquals("hourly", Iterables.getOnlyElement(tags.get("rollup")));

        when(metricNameSupplier.get()).thenReturn("my/metric/name_1d");
        tags = tagger.createTags(metricNameSupplier, tagsSupplier);
        assertEquals(1, tags.size());
        assertEquals("daily", Iterables.getOnlyElement(tags.get("rollup")));

        when(metricNameSupplier.get()).thenReturn("my/metric/name");
        tags = tagger.createTags(metricNameSupplier, tagsSupplier);
        assertEquals(1, tags.size());
        assertEquals("none", Iterables.getOnlyElement(tags.get("rollup")));
    }
}
