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

import com.arpnetworking.metrics.Event;
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.impl.ApacheHttpSink;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MetricReportingModule}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class MetricReportingModuleTest {

    @Test
    public void testParseTaggerBuildersEmpty() {
        assertTrue(MetricReportingModule.parseTaggerBuilders(new Properties()).isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void testParseTaggerBuildersDoesNotExist() {
        final Properties properties = new Properties();
        properties.put("kairosdb.reporter.tagger.foo.bar.class", "com.example.taggers.DoesNotExist");
        MetricReportingModule.parseTaggerBuilders(properties);
    }

    @Test
    public void testParseTaggerBuildersSingle() {
        final Properties properties = new Properties();
        properties.put("kairosdb.reporter.tagger.foo.bar.class", "org.kairosdb.core.reporting.NoTagsTagger");
        final Map<String, Class<?>> taggerBuilders = MetricReportingModule.parseTaggerBuilders(properties);
        assertEquals(1, taggerBuilders.size());
        assertTrue(taggerBuilders.containsKey("foo.bar"));
        assertEquals(NoTagsTagger.Builder.class, taggerBuilders.get("foo.bar"));
    }

    @Test
    public void testParseTaggerBuildersMultiple() {
        final Properties properties = new Properties();
        properties.put("kairosdb.reporter.tagger.foo.bar.class", "org.kairosdb.core.reporting.NoTagsTagger");
        properties.put("kairosdb.reporter.tagger.abc.class", "org.kairosdb.core.reporting.MetricNameTagger");
        final Map<String, Class<?>> taggerBuilders = MetricReportingModule.parseTaggerBuilders(properties);
        assertEquals(2, taggerBuilders.size());
        assertTrue(taggerBuilders.containsKey("foo.bar"));
        assertTrue(taggerBuilders.containsKey("abc"));
        assertEquals(NoTagsTagger.Builder.class, taggerBuilders.get("foo.bar"));
        assertEquals(MetricNameTagger.Builder.class, taggerBuilders.get("abc"));
    }

    @Test
    public void testLoadTaggersEmpty() {
        assertTrue(MetricReportingModule.loadTaggers(Collections.emptyMap(), new Properties()).isEmpty());
    }

    @Test
    public void testLoadTaggersSingleNoConfig() {
        final Properties properties = new Properties();
        final Map<String, Class<?>> taggerBuilders = Collections.singletonMap("foo.bar", NoTagsTagger.Builder.class);
        final Map<String, Tagger> taggers = MetricReportingModule.loadTaggers(taggerBuilders, properties);
        assertEquals(1, taggers.size());
        assertTrue(taggers.get("foo.bar") instanceof NoTagsTagger);
    }

    @Test
    public void testLoadTaggersSingleWithConfig() {
        final Properties properties = new Properties();
        properties.put("kairosdb.reporter.tagger.foo.bar.tags.0", "tagA");
        properties.put("kairosdb.reporter.tagger.foo.bar.tags.1", "tagB");
        properties.put("kairosdb.reporter.tagger.foo.bar.mappedTags.foo", "bar");
        final Map<String, Class<?>> taggerBuilders = Collections.singletonMap("foo.bar", TagTagger.Builder.class);
        final Map<String, Tagger> taggers = MetricReportingModule.loadTaggers(taggerBuilders, properties);
        assertEquals(1, taggers.size());
        assertTrue(taggers.get("foo.bar") instanceof TagTagger);
        final TagTagger tagTagger = (TagTagger) taggers.get("foo.bar");
        assertEquals("tagA", tagTagger.getTagMapping().get("tagA"));
        assertEquals("tagB", tagTagger.getTagMapping().get("tagB"));
        assertEquals("bar", tagTagger.getTagMapping().get("foo"));
    }

    @Test
    public void testParseSinkBuildersEmpty() {
        assertTrue(MetricReportingModule.parseSinkBuilders("").isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void testParseSinkBuildersDoesNotExist() {
        MetricReportingModule.parseSinkBuilders("com.example.sinks.DoesNotExist");
    }

    @Test
    public void testParseSinkBuildersSingle() {
        final List<Class<?>> builderClasses = MetricReportingModule.parseSinkBuilders(
                "org.kairosdb.core.reporting.CassandraSink");
        assertEquals(1, builderClasses.size());
        assertEquals("org.kairosdb.core.reporting.CassandraSink$Builder", builderClasses.get(0).getName());
    }

    @Test
    public void testParseSinkBuildersMultiple() {
        final List<Class<?>> builderClasses = MetricReportingModule.parseSinkBuilders(
                "org.kairosdb.core.reporting.CassandraSink ,  com.arpnetworking.metrics.impl.ApacheHttpSink");
        assertEquals(2, builderClasses.size());
        assertEquals("org.kairosdb.core.reporting.CassandraSink$Builder", builderClasses.get(0).getName());
        assertEquals("com.arpnetworking.metrics.impl.ApacheHttpSink$Builder", builderClasses.get(1).getName());
    }

    @Test
    public void testLoadSinksEmpty() {
        assertTrue(MetricReportingModule.loadSinks(Collections.emptyList(), new Properties()).isEmpty());
    }

    @Test
    public void testLoadSinksSingleNoConfig() {
        final Properties properties = new Properties();
        final List<Class<?>> sinkBuilders = Collections.singletonList(CassandraSink.Builder.class);
        final Map<String, Sink> sinks = MetricReportingModule.loadSinks(sinkBuilders, properties);
        assertEquals(1, sinks.size());
        assertTrue(sinks.get("kairosdb.reporter.sinks.0") instanceof CassandraSink);
    }

    @Test
    public void testLoadSinksSingleWithConfig() {
        final Properties properties = new Properties();
        properties.setProperty("kairosdb.reporter.sinks.0.foo", "bar");
        final List<Class<?>> sinkBuilders = Collections.singletonList(TestSink.Builder.class);
        final Map<String, Sink> sinks = MetricReportingModule.loadSinks(sinkBuilders, properties);
        assertEquals(1, sinks.size());
        assertTrue(sinks.get("kairosdb.reporter.sinks.0") instanceof TestSink);
        final TestSink testSink = (TestSink) sinks.get("kairosdb.reporter.sinks.0");
        assertEquals("bar", testSink.getFoo());
    }

    @Test
    public void testLoadSinksMultiple() {
        final Properties properties = new Properties();
        properties.setProperty("kairosdb.reporter.sinks.0.foo", "bar");
        properties.setProperty("kairosdb.reporter.sinks.1.maxBatchSize", "1234");
        final List<Class<?>> sinkBuilders = Arrays.asList(TestSink.Builder.class, ApacheHttpSink.Builder.class);
        final Map<String, Sink> sinks = MetricReportingModule.loadSinks(sinkBuilders, properties);
        assertEquals(2, sinks.size());
        assertTrue(sinks.get("kairosdb.reporter.sinks.0") instanceof TestSink);
        assertTrue(sinks.get("kairosdb.reporter.sinks.1") instanceof ApacheHttpSink);
    }

    /**
     * Test sink implementation.
     */
    public static final class TestSink implements Sink {

        private final String _foo;

        public TestSink(final Builder builder) {
            _foo = builder._foo;
        }

        public String getFoo() {
            return _foo;
        }

        @Override
        public void record(final Event event) {
            // Nothing to do
        }

        /**
         * Builder implementation for {@link TestSink} without the
         * Commons Builder pattern.
         */
        public static final class Builder {

            private String _foo;

            public Sink build() {
                return new TestSink(this);
            }

            public Builder setFoo(final String value) {
                _foo = value;
                return this;
            }
        }
    }
}
