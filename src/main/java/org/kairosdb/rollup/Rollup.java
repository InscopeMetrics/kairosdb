package org.kairosdb.rollup;

import com.google.gson.annotations.SerializedName;
import jakarta.validation.constraints.NotEmpty;
import org.kairosdb.core.datastore.QueryMetric;

import java.util.ArrayList;
import java.util.List;
import jakarta.validation.constraints.NotNull;

public class Rollup {
    private final transient List<QueryMetric> queryMetrics = new ArrayList<QueryMetric>();
    @NotNull
    @NotEmpty
    @SerializedName("save_as")
    private String saveAs;
    // todo add tags

    //	public Rollup(String saveAs, QueryMetric query)
    //	{
    //		// todo add checks for null and empty
    //		this.saveAs = saveAs;
    //		this.query = query;
    //	}

    public String getSaveAs() {
        return saveAs;
    }

    public List<QueryMetric> getQueryMetrics() {
        return queryMetrics;
    }

    public void addQueries(final List<QueryMetric> queries) {
        this.queryMetrics.addAll(queries);
    }

    public void addQuery(final QueryMetric query) {
        queryMetrics.add(query);
    }
}
