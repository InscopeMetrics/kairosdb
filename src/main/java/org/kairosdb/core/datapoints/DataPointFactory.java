package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.IOException;

/**
 * Implementation must be thread safe.
 */
public interface DataPointFactory {
    /**
     * This returns the type string that represents the data as it is packed in
     * binary form.  The string returned from this call will be stored with the
     * data and used to locate the appropriate factory to marshal the data when
     * retrieving data from the datastore.
     *
     * @return
     */
    String getDataStoreType();

    /**
     * This really is for aggregation purposes.  We know if an aggregator can handle
     * this type by checking the group type against the aggregator by calling
     * Aggregator.canAggregate().
     * <p>
     * As of this writing there are two group types used inside Kairos 'number' and
     * 'text'.  This is free formed and you can make up your own.
     *
     * @return
     */
    String getGroupType();

    DataPoint getDataPoint(long timestamp, JsonElement json) throws IOException;

    DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException;
}
