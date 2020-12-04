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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.verify;

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
        MockitoAnnotations.initMocks(this);
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
}
