package ru.loadtest.listeners.clickhouse.filter;

import org.apache.jmeter.samplers.SampleResult;

public interface ISamplersFilter {
    boolean isSamplerValid(SampleResult sampler);
}
