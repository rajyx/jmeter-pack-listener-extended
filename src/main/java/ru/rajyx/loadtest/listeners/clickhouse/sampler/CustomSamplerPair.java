package ru.rajyx.loadtest.listeners.clickhouse.sampler;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CustomSamplerPair)) return false;
        CustomSamplerPair that = (CustomSamplerPair) o;
        return Objects.equals(threadName, that.threadName) && Objects.equals(samplerLabel, that.samplerLabel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(threadName, samplerLabel);
    }
}
