//
// KairosMetricReporterListProvider.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.reporting;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class KairosMetricReporterListProvider implements TypeListener {
    private final Set<KairosMetricReporter> m_reporters = new HashSet<KairosMetricReporter>();

    public Set<KairosMetricReporter> get() {
        return (Collections.unmodifiableSet(m_reporters));
    }

    @Override
    public <I> void hear(final TypeLiteral<I> type, final TypeEncounter<I> encounter) {
        encounter.register(new InjectionListener<I>() {
            @Override
            public void afterInjection(final I injectee) {
                if (KairosMetricReporter.class.isAssignableFrom(injectee.getClass())) {
                    m_reporters.add((KairosMetricReporter) injectee);
                }
            }
        });
    }
}
