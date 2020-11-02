package org.kairosdb.core.datastore;

import java.util.Date;

public class ServiceKeyValue {
    private final String value;
    private final Date lastModified;

    public ServiceKeyValue(final String value, final Date lastModified) {
        this.value = value;
        this.lastModified = lastModified;
    }

    public String getValue() {
        return value;
    }

    public Date getLastModified() {
        return lastModified;
    }
}
