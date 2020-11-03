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

import org.apache.bval.constraints.NotEmpty;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.plugin.GroupBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

@FeatureComponent(
        name = "tag",
        description = "Groups data points by tag names."
)
public class TagGroupBy implements GroupBy {
    @NotNull
    @NotEmpty()
    @FeatureProperty(
            label = "Tags",
            description = "A list of tags to group by.",
            autocomplete = "tags",
            validations = {
                    @ValidationProperty(
                            expression = "value.length > 0",
                            message = "Tags can't be empty."
                    )
            }
    )
    private List<String> tags;

    public TagGroupBy() {
    }

    public TagGroupBy(final List<String> tagNames) {
        checkNotNull(tagNames);
        this.tags = new ArrayList<String>(tagNames);
    }

    public TagGroupBy(final String... tagNames) {
        this.tags = new ArrayList<String>();
        Collections.addAll(this.tags, tagNames);
    }

    @Override
    public int getGroupId(final DataPoint dataPoint, final Map<String, String> tags) {
        // Never used. Grouping by tags are done differently for performance reasons.
        return 0;
    }

    @Override
    public GroupByResult getGroupByResult(final int id) {
        // Never used. Grouping by tags are done differently for performance reasons.
        return null;
    }

    @Override
    public void setStartDate(final long startDate) {
    }

    /**
     * Returns the list of tag names to group by.
     *
     * @return list of tag names to group by
     */
    public List<String> getTagNames() {
        return Collections.unmodifiableList(tags);
    }

    public void setTags(final List<String> tags) {
        this.tags = tags;
    }
}