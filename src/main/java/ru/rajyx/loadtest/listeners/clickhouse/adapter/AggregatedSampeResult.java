package ru.rajyx.loadtest.listeners.clickhouse.adapter;

public class AggregatedSampeResult {
    private int errosCount;
    private long averageTime;
    private String threadName;
    private String sampleLabel;
    private int pointCount;

    public AggregatedSampeResult setErrorsCount(int errorsCount) {
        this.errosCount = errorsCount;
        return this;
    }

    public AggregatedSampeResult setAverageTime(long averageTime) {
        this.averageTime = averageTime;
        return this;
    }

    public AggregatedSampeResult setThreadName(String threadName) {
        this.threadName = threadName;
        return this;
    }

    public AggregatedSampeResult setSampleLabel(String sampleLabel) {
        this.sampleLabel = sampleLabel;
        return this;
    }

    public AggregatedSampeResult setPointCount(int pointCount) {
        this.pointCount = pointCount;
        return this;
    }

    public int getErrorsCount() {
        return errosCount;
    }

    public long getAverageTime() {
        return averageTime;
    }

    public String getThreadName() {
        return threadName;
    }

    public String getSampleLabel() {
        return sampleLabel;
    }

    public int getPointCount() {
        return pointCount;
    }
}
