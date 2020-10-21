package org.kairosdb.core.aggregator;

import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.Aggregator;

/**
 * Created by bhawkins on 5/18/17.
 */
public class DropAggregator implements Aggregator {
    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        return null;
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return false;
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return null;
    }

    public enum Drop {
        HIGH, LOW, BOTH
    }
}
