package org.kairosdb.core.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;

import java.util.List;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.validation.constraints.Null;

import static com.google.common.base.Preconditions.checkNotNull;

public class DatastoreQueryHealthCheck extends HealthCheck implements HealthStatus {
    static final String NAME = "Datastore-Query";
    private static final String METRIC_NAME_PROPERTY = "kairosdb.health.datastore.metric";
    private static final String TAGS_PROPERTY = "kairosdb.health.datastore.tags";

    private final KairosDatastore datastore;
    private final String metricName;
    private final SetMultimap<String, String> tags = HashMultimap.create();

    @Inject
    public DatastoreQueryHealthCheck(
            final KairosDatastore datastore,
            @Named(METRIC_NAME_PROPERTY) final String metricName,
            @Nullable @Named(TAGS_PROPERTY) final String tagString) {
        this.datastore = checkNotNull(datastore);
        this.metricName = checkNotNull(metricName);

        if (!Strings.isNullOrEmpty(tagString)) {
            final String[] parts = tagString.split(",");
            if (parts.length % 2 != 0) {
                throw new RuntimeException(
                        String.format(
                                "Tags must be comma separated key-value pairs; %s=%s",
                                TAGS_PROPERTY,
                                tagString));
            }
            for (int i = 0; i < parts.length; i +=2) {
                tags.put(parts[i].trim(), parts[i+1].trim());
            }
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected Result check() throws Exception {
        try (final DatastoreQuery query = datastore.createQuery(
                new QueryMetric(
                        System.currentTimeMillis() - (10 * 60 * 1000),
                        0,
                        metricName)
                        .setTags(tags))) {
            final List<DataPointGroup> results = query.execute();
            return Result.healthy();
        } catch (final DatastoreException e) {
            return Result.unhealthy(e);
        }
    }
}
