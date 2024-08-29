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

import com.google.common.collect.SetMultimap;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.function.Supplier;

import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Tests for {@link NoTagsTagger}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class NoTagsTaggerTest {

    private final AutoCloseable mocks;
    @Mock
    private Supplier<String> metricNameSupplier;
    @Mock
    private Supplier<SetMultimap<String, String>> tagsSupplier;

    public NoTagsTaggerTest() {
        mocks = MockitoAnnotations.openMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        mocks.close();
    }

    @Test
    public void test() {
        final Tagger tagger = new NoTagsTagger.Builder().build();
        tagger.applyTagsToThreadReporter(metricNameSupplier, tagsSupplier);
        verifyNoInteractions(metricNameSupplier);
        verifyNoInteractions(tagsSupplier);
    }
}
