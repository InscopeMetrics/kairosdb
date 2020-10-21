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
package org.kairosdb.core.groupby;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.GroupBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups data points based on a list of GroupBys.
 */
public class Grouper {
    private final KairosDataPointFactory m_dataPointFactory;

    public Grouper(final KairosDataPointFactory dataPointFactory) {
        m_dataPointFactory = dataPointFactory;
    }

    /**
     * Groups data points by group bys.
     *
     * @param groupBys           list of group bys
     * @param dataPointGroupList list of data point groups to group
     * @return list of data point groups
     */
    public List<DataPointGroup> group(final List<GroupBy> groupBys, final List<DataPointGroup> dataPointGroupList) throws IOException {
        if (groupBys.size() < 1)
            return dataPointGroupList;

        final List<DataPointGroup> dataPointGroups = new ArrayList<DataPointGroup>();
        for (final DataPointGroup dataPointGroup : dataPointGroupList) {
            final Map<List<Integer>, Group> groupIdsToGroup = new LinkedHashMap<List<Integer>, Group>();
            final Map<String, String> tags = getTags(dataPointGroup);

            while (dataPointGroup.hasNext()) {
                final DataPoint dataPoint = dataPointGroup.next();

                final List<Integer> groupIds = new ArrayList<Integer>();
                final List<GroupByResult> results = new ArrayList<GroupByResult>();
                for (final GroupBy groupBy : groupBys) {
                    final int groupId = groupBy.getGroupId(dataPoint, tags);
                    groupIds.add(groupId);
                    results.add(groupBy.getGroupByResult(groupId));
                }

                // add to group
                final Group group = getGroup(groupIdsToGroup, dataPointGroup, groupIds, results);
                group.addDataPoint(dataPoint);
            }

            for (final Group group : groupIdsToGroup.values()) {
                if (!dataPointGroup.getGroupByResult().isEmpty()) {
                    group.addGroupByResults(dataPointGroup.getGroupByResult());
                }
                dataPointGroups.add(group.getDataPointGroup());
            }

            dataPointGroup.close();
        }


        return dataPointGroups;
    }

    private Group getGroup(final Map<List<Integer>, Group> groupIdsToGroup, final DataPointGroup dataPointGroup, final List<Integer> groupIds, final List<GroupByResult> results) throws IOException {
        Group group = groupIdsToGroup.get(groupIds);
        if (group == null) {
            group = Group.createGroup(dataPointGroup, groupIds, results, m_dataPointFactory);
            groupIdsToGroup.put(groupIds, group);
        }
        return group;
    }

    private Map<String, String> getTags(final DataPointGroup dataPointGroup) {
        final Map<String, String> map = new LinkedHashMap<String, String>();
        for (final String tagName : dataPointGroup.getTagNames()) {
            map.put(tagName, dataPointGroup.getTagValues(tagName).iterator().next());
        }

        return map;
    }

}