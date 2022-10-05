package org.kairosdb.core.http.rest.json;

import java.util.HashMap;
import java.util.Map;

public class RollupResponse {
    private final String id;
    private final String name;
    private final Map<String, String> attributes = new HashMap<String, String>();

    public RollupResponse(final String id, final String name, final String url) {
        this.id = id;
        this.name = name;
        attributes.put("url", url);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
