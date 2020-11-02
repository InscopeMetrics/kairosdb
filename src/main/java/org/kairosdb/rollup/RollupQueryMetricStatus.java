package org.kairosdb.rollup;

public class RollupQueryMetricStatus {
    private final String metricName;
    private final String lastExecuted;
    private long dataPointCount;
    private final long executionLength;
    private String errorMessage;

    public RollupQueryMetricStatus(final String metricName, final String lastExecuted, final long dataPointCount, final long executionLength) {
        this.metricName = metricName;
        this.lastExecuted = lastExecuted;
        this.dataPointCount = dataPointCount;
        this.executionLength = executionLength;
    }

    public RollupQueryMetricStatus(final String metricName, final String lastExecuted, final long executionLength, final String errorMessage) {
        this.metricName = metricName;
        this.lastExecuted = lastExecuted;
        this.executionLength = executionLength;
        this.errorMessage = errorMessage;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getLastExecuted() {
        return lastExecuted;
    }

    public long getDataPointCount() {
        return dataPointCount;
    }

    public long getExecutionLength() {
        return executionLength;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean hasError() {
        return !errorMessage.isEmpty();
    }
}
