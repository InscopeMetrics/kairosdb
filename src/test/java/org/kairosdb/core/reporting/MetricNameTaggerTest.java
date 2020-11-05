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
 * Tests for {@link MetricNameTagger}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class MetricNameTaggerTest {

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

    public MetricNameTaggerTest() {
        MockitoAnnotations.initMocks(this);
        when(metricsFactory.create()).thenReturn(metrics);
        when(metricNameSupplier.get()).thenReturn("hows/my/metric/name");
    }

    @Test
    public void testThreadReporter() {
        ThreadReporter.initialize(metricsFactory);
        final MetricNameTagger tagger = new MetricNameTagger.Builder().build();
        tagger.applyTagsToThreadReporter(metricNameSupplier, tagsSupplier);
        ThreadReporter.close();

        verifyZeroInteractions(tagsSupplier);
        verify(metricNameSupplier).get();
        verify(metrics).addAnnotation("metricName", "hows/my/metric/name");
    }

    @Test
    public void testApplyMetricNameFull() {
        final MetricNameTagger tagger = new MetricNameTagger.Builder().build();
        tagger.applyMetricName(metricNameSupplier, tagConsumer);
        verifyZeroInteractions(tagsSupplier);
        verify(metricNameSupplier).get();
        verify(tagConsumer).accept("metricName", "hows/my/metric/name");
    }

    @Test
    public void testApplyMetricOneSegment() {
        final MetricNameTagger tagger = new MetricNameTagger.Builder()
                .setSegments(1)
                .build();
        tagger.applyMetricName(metricNameSupplier, tagConsumer);
        verifyZeroInteractions(tagsSupplier);
        verify(metricNameSupplier).get();
        verify(tagConsumer).accept("metricName", "hows");
    }

    @Test
    public void testApplyMetricTwoSegments() {
        final MetricNameTagger tagger = new MetricNameTagger.Builder()
                .setSegments(2)
                .build();
        tagger.applyMetricName(metricNameSupplier, tagConsumer);
        verifyZeroInteractions(tagsSupplier);
        verify(metricNameSupplier).get();
        verify(tagConsumer).accept("metricName", "hows/my");
    }

    @Test
    public void testApplyMetricAlternateSplitter() {
        final MetricNameTagger tagger = new MetricNameTagger.Builder()
                .setSplitter("m")
                .setSegments(3)
                .build();
        tagger.applyMetricName(metricNameSupplier, tagConsumer);
        verifyZeroInteractions(tagsSupplier);
        verify(metricNameSupplier).get();
        verify(tagConsumer).accept("metricName", "hows/my/metric/na");
    }
}
