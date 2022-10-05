/*
 * Copyright 2016 KairosDB Authors
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
package org.kairosdb.util;


import org.kairosdb.core.datastore.Order;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class TournamentTree<T> {
    //===========================================================================
    private final TreeSet<TreeValue<T>> m_treeSet;
    private final Comparator<T> m_comparator;
    private int m_iteratorIndex = 0;
    private final Order m_order;

    public TournamentTree(final Comparator<T> comparator, final Order order) {
        m_comparator = comparator;
        m_treeSet = new TreeSet<TreeValue<T>>(new TreeComparator());
        m_order = order;
    }

    //---------------------------------------------------------------------------
    public void addIterator(final Iterator<T> iterator) {
        if (iterator.hasNext())
            m_treeSet.add(new TreeValue<T>(iterator, iterator.next(), m_iteratorIndex++));
    }

    //---------------------------------------------------------------------------
    public boolean hasNext() {
        return !m_treeSet.isEmpty();
    }

    //---------------------------------------------------------------------------
    public T nextElement() {
        final TreeValue<T> value;
        if (m_order == Order.ASC)
            value = m_treeSet.pollFirst();
        else
            value = m_treeSet.pollLast();

        if (value == null)
            return (null);

        final T ret = value.getValue();

        if (value.getIterator().hasNext()) {
            value.setValue(value.getIterator().next());
            m_treeSet.add(value);
        }

        return (ret);
    }

    private static class TreeValue<T> {
        private final int m_iteratorNum;
        private T m_value;
        private final Iterator<T> m_iterator;

        public TreeValue(final Iterator<T> iterator, final T value, final int iteratorNum) {
            m_iterator = iterator;
            m_value = value;
            m_iteratorNum = iteratorNum;
        }

        public int getIteratorNum() {
            return (m_iteratorNum);
        }

        public T getValue() {
            return (m_value);
        }

        public void setValue(final T value) {
            m_value = value;
        }

        public Iterator<T> getIterator() {
            return (m_iterator);
        }
    }

    private class TreeComparator implements Comparator<TreeValue<T>> {
        public int compare(final TreeValue<T> tv1, final TreeValue<T> tv2) {
            final int resp = m_comparator.compare(tv1.getValue(), tv2.getValue());

            if (resp == 0)
                return (tv1.getIteratorNum() - tv2.getIteratorNum());
            else
                return (resp);
        }
    }
}
