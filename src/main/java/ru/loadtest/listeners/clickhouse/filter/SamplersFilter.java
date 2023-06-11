package ru.loadtest.listeners.clickhouse.filter;

import org.apache.jmeter.samplers.Sampler;

import java.util.Set;

public class SamplersFilter implements ISamplersFilter {
    private String filterRegex;
    private Set<String> samplersToStore;

    public SamplersFilter setFilterRegex(String filterRegex) {
        this.filterRegex = filterRegex;
        return this;
    }

    public SamplersFilter setSamplersToStore(Set<String> samplersToStore) {
        this.samplersToStore = samplersToStore;
        return this;
    }

    @Override
    public boolean isSamplerForbidden(Sampler sampler) {
        return false;
    }
}
