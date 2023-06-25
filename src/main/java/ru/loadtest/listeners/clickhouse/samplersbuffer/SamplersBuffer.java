package ru.loadtest.listeners.clickhouse.samplersbuffer;

import org.apache.jmeter.samplers.SampleResult;
import ru.loadtest.listeners.clickhouse.filter.ISamplersFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SamplersBuffer implements ISamplersBuffer {
    private List<SampleResult> allSampleResults;
    private ISamplersFilter samplersFilter;

    private boolean recordSubSamplers;

    public SamplersBuffer(ISamplersFilter samplersFilter, boolean recordSubSamplers) {
        this.samplersFilter = samplersFilter;
        this.recordSubSamplers = recordSubSamplers;
        this.allSampleResults = new ArrayList<>();
    }

    @Override
    public void addSamplers(List<SampleResult> sampleResults) {
        for (int i = 0; i < sampleResults.size(); i++) {
            SampleResult sample = sampleResults.get(i);
            if (samplersFilter.isSamplerValid(sample)) {
                allSampleResults.add(sample);
            }
            List<SampleResult> subSamplers = Arrays.asList(sample.getSubResults());
            if (recordSubSamplers && subSamplers.size() > 0) {
                addSamplers(subSamplers);
            }
        }
    }

    @Override
    public List<SampleResult> getSampleResults() {
        return allSampleResults;
    }

    @Override
    public void clearBuffer() {
        allSampleResults.clear();
    }

}
