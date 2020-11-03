package org.kairosdb.core;

import com.google.gson.JsonElement;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 12/9/13
 * Time: 8:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestDataPointFactory implements KairosDataPointFactory {
    private final Map<String, DataPointFactory> m_factoryMapDataStore = new HashMap<String, DataPointFactory>();
    private final Map<String, DataPointFactory> m_factoryMapRegistered = new HashMap<String, DataPointFactory>();

    public TestDataPointFactory() {
        addFactory("long", new LongDataPointFactoryImpl());
        addFactory("double", new DoubleDataPointFactoryImpl());
        addFactory("legacy", new LegacyDataPointFactory());
    }

    private void addFactory(final String type, final DataPointFactory factory) {
        m_factoryMapRegistered.put(type, factory);
        m_factoryMapDataStore.put(factory.getDataStoreType(), factory);
    }

    @Override
    public DataPoint createDataPoint(final String type, final long timestamp, final JsonElement json) throws IOException {
        final DataPointFactory factory = m_factoryMapRegistered.get(type);

        final DataPoint dp = factory.getDataPoint(timestamp, json);

        return (dp);
    }

    @Override
    public DataPoint createDataPoint(final String type, final long timestamp, final DataInput buffer) throws IOException {
        final DataPointFactory factory = m_factoryMapDataStore.get(type);

        final DataPoint dp = factory.getDataPoint(timestamp, buffer);

        return (dp);
    }

    @Override
    public DataPointFactory getFactoryForType(final String type) {
        return m_factoryMapRegistered.get(type);
    }

    @Override
    public DataPointFactory getFactoryForDataStoreType(final String dataStoreType) {
        return m_factoryMapDataStore.get(dataStoreType);
    }

    @Override
    public String getGroupType(final String datastoreType) {
        return getFactoryForDataStoreType(datastoreType).getGroupType();
    }

    @Override
    public boolean isRegisteredType(final String type) {
        return m_factoryMapRegistered.containsKey(type);
    }
}
