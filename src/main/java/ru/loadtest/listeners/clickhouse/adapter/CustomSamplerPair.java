package ru.loadtest.listeners.clickhouse.adapter;

public class CustomSamplerPair {
    private final String threadName;
    private final String samplerLabel;

    public CustomSamplerPair(String threadName, String samplerLabel) {
        this.threadName = threadName;
        this.samplerLabel = samplerLabel;
    }

    public String getThreadName() {
        return threadName;
    }

    public String getSamplerLabel() {
        return samplerLabel;
    }
}
