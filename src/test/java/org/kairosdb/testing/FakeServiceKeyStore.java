package org.kairosdb.testing;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FakeServiceKeyStore implements ServiceKeyStore {
    private final Table<String, String, Map<String, ServiceKeyValue>> table = HashBasedTable.create();
    private final Map<String, Date> timeStamps = new HashMap<>();

    @SuppressWarnings("Java8MapApi")
    @Override
    public void setValue(final String service, final String serviceKey, final String key, final String value)
            throws DatastoreException {
        final Date now = new Date();
        final Map<String, ServiceKeyValue> keyMap = getKeyMap(service, serviceKey);
        keyMap.put(key, new ServiceKeyValue(value, now));
        timeStamps.put(service + "_" + serviceKey, now);
    }

    @Override
    public ServiceKeyValue getValue(final String service, final String serviceKey, final String key)
            throws DatastoreException {
        final Map<String, ServiceKeyValue> keyMap = getKeyMap(service, serviceKey);
        return keyMap.get(key);
    }

    @Override
    public Iterable<String> listServiceKeys(final String service)
            throws DatastoreException {
        final Map<String, Map<String, Map<String, ServiceKeyValue>>> rowMap = table.rowMap();
        return rowMap.keySet();
    }

    @SuppressWarnings("Java8MapApi")
    @Override
    public Iterable<String> listKeys(final String service, final String serviceKey)
            throws DatastoreException {
        final Map<String, ServiceKeyValue> map = table.get(service, serviceKey);
        if (map != null) {
            return map.keySet();
        }
        return ImmutableSet.of();
    }

    @Override
    public Iterable<String> listKeys(final String service, final String serviceKey, final String keyStartsWith)
            throws DatastoreException {
        final Set<String> keys = getKeyMap(service, serviceKey).keySet();
        for (final String key : keys) {
            if (key.startsWith(keyStartsWith)) {
                keys.add(key);
            }
        }
        return keys;
    }

    @Override
    public void deleteKey(final String service, final String serviceKey, final String key)
            throws DatastoreException {
        final Map<String, ServiceKeyValue> keyMap = getKeyMap(service, serviceKey);
        keyMap.remove(key);
        timeStamps.put(service + "_" + serviceKey, new Date());
    }

    @Override
    public Date getServiceKeyLastModifiedTime(final String service, final String serviceKey)
            throws DatastoreException {
        return timeStamps.get(service + "_" + serviceKey);
    }

    public void setKeyModificationTime(final String service, final String serviceKey, final String key, final Date lastModfiied) {
        final Map<String, ServiceKeyValue> keyMap = getKeyMap(service, serviceKey);
        final ServiceKeyValue value = keyMap.get(key);
        if (value != null) {
            keyMap.put(key, new ServiceKeyValue(value.getValue(), lastModfiied));
        }
    }

    private Map<String, ServiceKeyValue> getKeyMap(final String service, final String serviceKey) {
        Map<String, ServiceKeyValue> keyMap = table.get(service, serviceKey);
        if (keyMap == null) {
            keyMap = new HashMap<>();
            table.put(service, serviceKey, keyMap);
        }
        return keyMap;
    }

}
