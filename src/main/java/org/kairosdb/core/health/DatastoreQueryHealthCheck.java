package org.kairosdb.core.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class DatastoreQueryHealthCheck extends HealthCheck implements HealthStatus {
    static final String NAME = "Datastore-Query";
    private final KairosDatastore datastore;

    @Inject
    public DatastoreQueryHealthCheck(final KairosDatastore datastore) {
        this.datastore = checkNotNull(datastore);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected Result check() throws Exception {
        // TODO(ville): Make the query configurable
        try (final DatastoreQuery query = datastore.createQuery(
                new QueryMetric(System.currentTimeMillis() - (10 * 60 * 1000),
                        0, "jvm/threads/thread_count"))) {
            final List<DataPointGroup> results = query.execute();
            return Result.healthy();
        } catch (final DatastoreException e) {
            return Result.unhealthy(e);
        }
    }
}
