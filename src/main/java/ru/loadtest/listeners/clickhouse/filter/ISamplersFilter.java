package ru.loadtest.listeners.clickhouse.filter;

import org.apache.jmeter.samplers.SampleResult;

import java.util.Set;

public interface ISamplersFilter {
    boolean isSamplerValid(SampleResult sampler);

    SamplersFilter setFilterRegex(String filterRegex);

    SamplersFilter setSamplersToStore(Set<String> samplersToStore);
}
