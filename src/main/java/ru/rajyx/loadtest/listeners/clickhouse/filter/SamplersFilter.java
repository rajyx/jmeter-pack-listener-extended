package ru.rajyx.loadtest.listeners.clickhouse.filter;

import org.apache.jmeter.samplers.SampleResult;

import java.util.Set;

public class SamplersFilter implements ISamplersFilter {
    private String filterRegex;
    private Set<String> samplersToStore;

    @Override
    public SamplersFilter setFilterRegex(String filterRegex) {
        this.filterRegex = filterRegex;
        return this;
    }

    @Override
    public SamplersFilter setSamplersToStore(Set<String> samplersToStore) {
        this.samplersToStore = samplersToStore;
        return this;
    }

    @Override
    public boolean isSamplerValid(SampleResult sampler) {
        String samplerLabel = sampler.getSampleLabel();
        if (samplersToStore != null) {
            return samplersToStore.contains(samplerLabel);
        }
        return samplerLabel.matches(filterRegex);
    }
}
