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

package org.kairosdb.datastore.cassandra;

import com.arpnetworking.metrics.incubator.PeriodicMetrics;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This cache serves two purposes.
 * 1.  Cache recently inserted data
 * 2.  Create a unique store of cached data.
 * <p>
 * The primary use of this class is to store row keys so we know if the row key
 * index needs to be updated or not.  Because it uniquely stores row keys we
 * can use the same row key object over and over.  With row keys we store the
 * serialized form of the key so we only have to serialize a row key once.
 * <p>
 * The data type must implement hashcode and equal methods.
 */
public class DataCache<T> {
    private final String name;

    private long hitCount = 0;
    private long missCount = 0;
    private long evictedCount = 0;
    private long expiredCount = 0;

    private final LinkItem<T> front = new LinkItem<T>(null);
    private final LinkItem<T> back = new LinkItem<T>(null);

    private final int maxSize;
    private final ConcurrentHashMap<T, LinkItem<T>> hashMap;

    public DataCache(final String name, final int cacheSize, final PeriodicMetrics periodicMetrics) {
        this.name = name;
        periodicMetrics.registerPolledMetric(this::recordMetrics);

        hashMap = new ConcurrentHashMap<>();
        maxSize = cacheSize;

        front.next = back;
        back.prev = front;
    }

    public synchronized T get(final T cacheData) {
        final LinkItem<T> mappedItem = getMappedItemAndUpdateLRU(cacheData);

        if (mappedItem != null && mappedItem.data != null) {
            ++hitCount;
            return mappedItem.data;
        }
        ++missCount;
        return null;
    }

    public synchronized void put(final T cacheData) {
        final LinkItem<T> existing = hashMap.get(cacheData);
        if (existing != null) {
            return;
        }

        final LinkItem<T> li = new LinkItem<>(cacheData);
        addLRUItem(li);
        hashMap.put(cacheData, li);
        pruneCache();
    }

    public synchronized Set<T> getCachedKeys() {
        return (hashMap.keySet());
    }

    public synchronized void removeKey(final T key) {
        final LinkItem<T> li = hashMap.remove(key);
        if (li != null) {
            ++expiredCount;
            removeLRUItem(li);
        }
    }

    public synchronized void clear() {
        front.next = back;
        back.prev = front;

        hashMap.clear();
    }

    private synchronized void recordMetrics(final PeriodicMetrics periodicMetrics) {
        periodicMetrics.recordGauge("data_cache/" + name + "/size", hashMap.size());
        periodicMetrics.recordGauge("data_cache/" + name + "/hits", hitCount);
        periodicMetrics.recordGauge("data_cache/" + name + "/misses", missCount);
        periodicMetrics.recordGauge("data_cache/" + name + "/expired", expiredCount);
        periodicMetrics.recordGauge("data_cache/" + name + "/evicted", evictedCount);

        hitCount = 0;
        missCount = 0;
        expiredCount = 0;
        evictedCount = 0;
    }

    private synchronized LinkItem<T> getMappedItemAndUpdateLRU(final T cacheData) {
        final LinkItem<T> mappedItem = hashMap.get(cacheData);

        if (mappedItem != null) {
            //moves item to top of list
            removeLRUItem(mappedItem);
            addLRUItem(mappedItem);
        }

        return mappedItem;
    }


    private synchronized void pruneCache() {
        while (hashMap.size() > maxSize) {
            final LinkItem<T> last = back.prev;
            removeLRUItem(last);

            hashMap.remove(last.data);
            ++evictedCount;
        }
    }

    private synchronized void removeLRUItem(final LinkItem<T> li) {
        li.prev.next = li.next;
        li.next.prev = li.prev;
    }

    private synchronized void addLRUItem(final LinkItem<T> li) {
        li.prev = front;
        li.next = front.next;

        front.next = li;
        li.next.prev = li;
    }

    private class LinkItem<T> {
        private final T data;
        private LinkItem<T> prev;
        private LinkItem<T> next;

        public LinkItem(final T data) {
            this.data = data;
        }
    }
}
