package org.kairosdb.core.aggregator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.core.groupby.TagGroupBy;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by bhawkins on 8/28/15.
 */
@FeatureComponent(
        name = "save_as",
        description = "Saves the results to a new metric.")
public class SaveAsAggregator implements Aggregator, GroupByAware {
    private static final String SAVED_FROM = "saved_from";
    private final Publisher<DataPointEvent> m_publisher;
    private Map<String, String> m_tags;
    private int m_ttl = 0;
    private final Set<String> m_tagsToKeep = new HashSet<>();
    private boolean m_addSavedFrom = true;
    @FeatureProperty(
            name = "metric_name",
            label = "Save As",
            description = "The name of the new metric.",
            default_value = "<new name>",
            validations = {
                    @ValidationProperty(
                            expression = "!value && value.length > 0",
                            message = "The name can't be empty."
                    )
            }
    )
    private String m_metricName;


    @Inject
    public SaveAsAggregator(final FilterEventBus eventBus) {
        m_publisher = eventBus.createPublisher(DataPointEvent.class);
        m_tags = new HashMap<>();
    }


    public void setAddSavedFrom(final boolean addSavedFrom) {
        m_addSavedFrom = addSavedFrom;
    }

    public void setTtl(final int ttl) {
        m_ttl = ttl;
    }

    public String getMetricName() {
        return m_metricName;
    }

    public void setMetricName(final String metricName) {
        m_metricName = metricName;
    }

    public Map<String, String> getTags() {
        return m_tags;
    }

    public void setTags(final Map<String, String> tags) {
        m_tags = tags;
    }

    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        return new SaveAsDataPointAggregator(dataPointGroup);
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return true;
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return groupType;
    }

    @Override
    public void setGroupBys(final List<GroupBy> groupBys) {
        for (final GroupBy groupBy : groupBys) {
            if (groupBy instanceof TagGroupBy) {
                final TagGroupBy tagGroupBy = (TagGroupBy) groupBy;

                m_tagsToKeep.addAll(tagGroupBy.getTagNames());
            }
        }
    }

    @VisibleForTesting
    public Set<String> getTagsToKeep() {
        return m_tagsToKeep;
    }

    private class SaveAsDataPointAggregator implements DataPointGroup {
        private final DataPointGroup m_innerDataPointGroup;
        private final ImmutableSortedMap<String, String> m_groupTags;

        public SaveAsDataPointAggregator(final DataPointGroup innerDataPointGroup) {
            m_innerDataPointGroup = innerDataPointGroup;
            final ImmutableSortedMap.Builder<String, String> mapBuilder = ImmutableSortedMap.naturalOrder();

            for (Map.Entry<String, String> tag : m_tags.entrySet()) {
                if (!tag.getKey().equals(SAVED_FROM) || !m_addSavedFrom) {
                    // If saved_from is specified, overwrite it below rather than from
                    // the tags aggregator argument.
                    mapBuilder.put(tag);
                }
            }

            if (m_addSavedFrom)
                mapBuilder.put(SAVED_FROM, innerDataPointGroup.getName());

            for (final String innerTag : innerDataPointGroup.getTagNames()) {
                if (!innerTag.equals(SAVED_FROM) || !m_addSavedFrom) {
                    final Set<String> tagValues = innerDataPointGroup.getTagValues(innerTag);
                    if (m_tagsToKeep.contains(innerTag) && (tagValues.size() == 1))
                        mapBuilder.put(innerTag, tagValues.iterator().next());
                }
            }

            m_groupTags = mapBuilder.build();
        }

        @Override
        public boolean hasNext() {
            return m_innerDataPointGroup.hasNext();
        }

        @Override
        public DataPoint next() {
            final DataPoint next = m_innerDataPointGroup.next();

            m_publisher.post(new DataPointEvent(m_metricName, m_groupTags, next, m_ttl));

            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return m_innerDataPointGroup.getName();
        }

        @Override
        public List<GroupByResult> getGroupByResult() {
            return m_innerDataPointGroup.getGroupByResult();
        }

        @Override
        public void close() {
            m_innerDataPointGroup.close();
        }

        @Override
        public Set<String> getTagNames() {
            return m_innerDataPointGroup.getTagNames();
        }

        @Override
        public Set<String> getTagValues(final String tag) {
            return m_innerDataPointGroup.getTagValues(tag);
        }
    }
}
