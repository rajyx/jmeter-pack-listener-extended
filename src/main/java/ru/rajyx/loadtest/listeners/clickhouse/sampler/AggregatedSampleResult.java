package ru.rajyx.loadtest.listeners.clickhouse.sampler;

import org.apache.jmeter.samplers.SampleResult;

public class AggregatedSampleResult extends SampleResult {
    private int errorCount;
    private double averageElapsedTime;

    @Override
    public void setErrorCount(int errorsCount) {
        this.errorCount = errorsCount;
    }

    public void setAverageElapsedTime(double averageElapsedTime) {
        this.averageElapsedTime = averageElapsedTime;
    }

    @Override
    public int getErrorCount() {
        return errorCount;
    }

    @Override
    public long getTime() {
        return (long) averageElapsedTime;
    }
}
