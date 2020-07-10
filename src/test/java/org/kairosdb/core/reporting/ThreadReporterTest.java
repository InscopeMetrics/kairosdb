package org.kairosdb.core.reporting;

import com.google.inject.Guice;
import com.google.inject.name.Names;
import com.google.inject.servlet.ServletModule;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


public class ThreadReporterTest {
    @Test
    public void testInjectionWhenTtlSpecified() throws Exception
    {
        final ArrayList<Integer> ttls = Lists.newArrayList(12345, 0, 20);
        for (final int ttl : ttls) {
            // given
            injectorWithTtl(ttl);
            // when
            final ThreadReporter.ReporterDataPoint dataPoint = ThreadReporter.addDataPoint("metric name", "value");
            // then
            assertThat(dataPoint.getTtl(), equalTo(ttl));
        }
    }

    @After
    public void afterEachTest() {
        injectorWithTtl(0);
    }

    private void injectorWithTtl(final Integer ttl) {
        Guice.createInjector(new ServletModule() {
            @Override
            protected void configureServlets() {
                if (ttl != null) {
                    bind(Integer.TYPE).annotatedWith(Names.named("kairosdb.reporter.ttl")).toInstance(ttl);
                }
                requestStaticInjection(ThreadReporter.class);
            }
        });
    }
}
