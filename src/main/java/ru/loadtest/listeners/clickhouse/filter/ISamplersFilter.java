package ru.loadtest.listeners.clickhouse.filter;

import org.apache.jmeter.samplers.Sampler;

public interface ISamplersFilter {
    boolean isSamplerForbidden(Sampler sampler);
}
