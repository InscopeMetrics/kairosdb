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

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractDataPointGroup implements DataPointGroup {
    private final String name;
    private TreeMultimap<String, String> tags = TreeMultimap.create();
    private final List<GroupByResult> groupByResult = new ArrayList<>();

    public AbstractDataPointGroup(final String name) {
        this.name = name;
    }

    public AbstractDataPointGroup(final String name, final SetMultimap<String, String> tags) {
        this.name = Preconditions.checkNotNullOrEmpty(name);
        this.tags = TreeMultimap.create(tags);
    }

    public void addTag(final String name, final String value) {
        tags.put(name, value);
    }

    public void addTags(final SetMultimap<String, String> tags) {
        this.tags.putAll(tags);
    }

    public void addTags(final Map<String, String> tags) {
        this.tags.putAll(Multimaps.forMap(tags));
    }

    public void addTags(final DataPointGroup dpGroup) {
        for (final String key : dpGroup.getTagNames()) {
            for (final String value : dpGroup.getTagValues(key)) {
                this.tags.put(key, value);
            }
        }
    }

    public void addGroupByResult(final GroupByResult groupByResult) {
        this.groupByResult.add(checkNotNull(groupByResult));
    }

    public List<GroupByResult> getGroupByResult() {
        return groupByResult;
    }

    public String getName() {
        return name;
    }

    @Override
    public Set<String> getTagNames() {
        return (tags.keySet());
    }

    @Override
    public Set<String> getTagValues(final String tag) {
        return (tags.get(tag));
    }

    public SetMultimap<String, String> getTags() {
        return (ImmutableSetMultimap.copyOf(tags));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public abstract void close();
}