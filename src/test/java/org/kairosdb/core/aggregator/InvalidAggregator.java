package org.kairosdb.core.aggregator;

import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.Aggregator;

public class InvalidAggregator implements Aggregator {
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
}
