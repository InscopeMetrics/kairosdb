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
package org.kairosdb.core.datastore;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;
import org.kairosdb.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryMetric implements DatastoreMetricQuery {
    private long startTime;
    private long endTime;
    private boolean endTimeSet;
    private final int cacheTime;
    private final String name;
    private SetMultimap<String, String> tags = HashMultimap.create();
    private final List<GroupBy> groupBys = new ArrayList<>();
    private final List<Aggregator> aggregators;
    private String cacheString;
    private boolean excludeTags = false;
    private int limit;
    private Order order = Order.ASC;
    private final List<QueryPlugin> plugins;

    public QueryMetric(final long start_time, final int cacheTime, final String name) {
        this.aggregators = new ArrayList<>();
        this.plugins = new ArrayList<>();
        this.startTime = start_time;
        this.cacheTime = cacheTime;
        this.name = Preconditions.checkNotNullOrEmpty(name);
    }

    public QueryMetric(final long start_time, final long end_time, final int cacheTime, final String name) {
        this.aggregators = new ArrayList<>();
        this.plugins = new ArrayList<>();
        this.startTime = start_time;
        this.endTime = end_time;
        this.endTimeSet = true;
        this.cacheTime = cacheTime;
        this.name = Preconditions.checkNotNullOrEmpty(name);
    }

    public QueryMetric addAggregator(final Aggregator aggregator) {
        checkNotNull(aggregator);

        this.aggregators.add(aggregator);
        return (this);
    }

    public QueryMetric addTag(final String name, final String value) {
        this.tags.put(name, value);
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    public List<Aggregator> getAggregators() {
        return aggregators;
    }

    @Override
    public SetMultimap<String, String> getTags() {
        return (tags);
    }

    public QueryMetric setTags(final SetMultimap<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public QueryMetric setTags(final Map<String, String> tags) {
        this.tags.clear();

        for (final String s : tags.keySet()) {
            this.tags.put(s, tags.get(s));
        }

        return this;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }

    @Override
    public long getEndTime() {
        if (!endTimeSet)
            endTime = Long.MAX_VALUE;

        return endTime;
    }

    public void setEndTime(final long endTime) {
        this.endTime = endTime;
        this.endTimeSet = true;
    }

    public int getCacheTime() {
        return cacheTime;
    }

    public List<GroupBy> getGroupBys() {
        return Collections.unmodifiableList(groupBys);
    }

    public void addGroupBy(final GroupBy groupBy) {
        this.groupBys.add(groupBy);
    }

    public String getCacheString() {
        return (cacheString);
    }

    public void setCacheString(final String cacheString) {
        this.cacheString = cacheString;
    }

    public boolean isExcludeTags() {
        return excludeTags;
    }

    public void setExcludeTags(final boolean excludeTags) {
        this.excludeTags = excludeTags;
    }

    public int getLimit() {
        return (limit);
    }

    public void setLimit(final int limit) {
        this.limit = limit;
    }

    public Order getOrder() {
        return (order);
    }

    public void setOrder(final Order order) {
        this.order = order;
    }

    @Override
    public List<QueryPlugin> getPlugins() {
        return Collections.unmodifiableList(plugins);
    }

    public void addPlugin(final QueryPlugin plugin) {
        this.plugins.add(plugin);
    }

    @Override
    public String toString() {
        return "QueryMetric{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", endTimeSet=" + endTimeSet +
                ", cacheTime=" + cacheTime +
                ", name='" + name + '\'' +
                ", tags=" + tags +
                ", groupBys=" + groupBys +
                ", aggregators=" + aggregators +
                ", cacheString='" + cacheString + '\'' +
                ", excludeTags=" + excludeTags +
                ", limit=" + limit +
                ", order=" + order +
                ", plugins=" + plugins +
                '}';
    }
}