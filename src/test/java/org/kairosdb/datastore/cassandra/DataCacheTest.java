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
import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DataCacheTest {

    @Test
    public void test_isCached() {
        final PeriodicMetrics periodicMetrics = Mockito.mock(PeriodicMetrics.class);
        final DataCache<String> cache = new DataCache<>("test_isCached", 3, periodicMetrics);

        cache.put("one");
        cache.put("two");
        cache.put("three");

        cache.put("one");
        assertNotNull(cache.get("one")); //This puts 'one' as the newest
        cache.put("four"); //This should boot out 'two'
        assertNull(cache.get("two")); //Should have booted 'three'
        cache.put("one");
        cache.put("three"); //Should have booted 'four'
        assertNotNull(cache.get("one"));
    }

    @Test
    public void test_uniqueCache() {
        final TestObject td1 = new TestObject("td1");
        final TestObject td2 = new TestObject("td2");
        final TestObject td3 = new TestObject("td3");

        final PeriodicMetrics periodicMetrics = Mockito.mock(PeriodicMetrics.class);
        final DataCache<TestObject> cache = new DataCache<TestObject>("test_uniqueCache", 10, periodicMetrics);

        cache.put(td1);
        cache.put(td2);
        cache.put(td3);

        TestObject ret = cache.get(new TestObject("td1"));
        assertTrue(td1 == ret);

        ret = cache.get(new TestObject("td2"));
        assertTrue(td2 == ret);

        ret = cache.get(new TestObject("td3"));
        assertTrue(td3 == ret);
    }

    public class TestObject {
        private final String m_data;

        public TestObject(final String data) {
            m_data = data;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final TestObject that = (TestObject) o;

            return m_data.equals(that.m_data);
        }

        @Override
        public int hashCode() {
            return m_data.hashCode();
        }
    }
}
