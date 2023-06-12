package ru.loadtest.listeners.clickhouse.samplersbuffer;

import org.apache.jmeter.samplers.SampleResult;

import java.util.List;

public interface ISamplersBuffer {
    void addSamplers(List<SampleResult> sampleResultList);

    List<SampleResult> getSampleResults();
}
