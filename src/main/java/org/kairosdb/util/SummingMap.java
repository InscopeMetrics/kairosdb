package org.kairosdb.util;

import java.util.HashMap;

public class SummingMap extends HashMap<String, Long> {
    private static final long serialVersionUID = -8116874754812985816L;

    @Override
    public Long put(final String key, final Long value) {
        if (super.containsKey(key)) {
            Long sum = super.get(key);
            sum += value;
            return super.put(key, sum);
        } else {
            return super.put(key, value);
        }
    }

    public String getKeyForSmallestValue() {
        Long smallest = Long.MAX_VALUE;
        String smallestKey = null;
        for (final Entry<String, Long> entry : super.entrySet()) {
            if (entry.getValue() < smallest) {
                smallest = entry.getValue();
                smallestKey = entry.getKey();
            }
        }

        return smallestKey;
    }
}
