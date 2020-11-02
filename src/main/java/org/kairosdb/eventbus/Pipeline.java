package org.kairosdb.eventbus;

import com.google.common.collect.ImmutableSortedSet;

import java.util.Collection;
import java.util.Iterator;

public class Pipeline implements Iterable<FilterSubscriber> {
    private final Object m_lock;
    private volatile ImmutableSortedSet<FilterSubscriber> m_pipeline;

    public Pipeline() {
        m_lock = new Object();
        m_pipeline = ImmutableSortedSet.<FilterSubscriber>naturalOrder().build();
    }

    @Override
    public Iterator<FilterSubscriber> iterator() {
        return m_pipeline.iterator();
    }

    public void addAll(final Collection<FilterSubscriber> subscribers) {
        synchronized (m_lock) {
            m_pipeline = ImmutableSortedSet.<FilterSubscriber>naturalOrder()
                    .addAll(m_pipeline)
                    .addAll(subscribers).build();
        }
    }

    public int size() {
        return m_pipeline.size();
    }

    public boolean isEmpty() {
        return m_pipeline.isEmpty();
    }
}
