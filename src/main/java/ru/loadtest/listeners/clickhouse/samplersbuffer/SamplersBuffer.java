package ru.loadtest.listeners.clickhouse.samplersbuffer;

import org.apache.jmeter.samplers.SampleResult;
import ru.loadtest.listeners.clickhouse.filter.ISamplersFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SamplersBuffer implements ISamplersBuffer {
    private List<SampleResult> allSampleResults;
    private ISamplersFilter samplersFilter;
    private String recordDataLevel;

    private boolean recordSubSamplers;

    public SamplersBuffer(ISamplersFilter samplersFilter, String recordDataLevel, boolean recordSubSamplers) {
        this.samplersFilter = samplersFilter;
        this.recordDataLevel = recordDataLevel;
        this.recordSubSamplers = recordSubSamplers;
        this.allSampleResults = new ArrayList<>();
    }

    @Override
    public void addSamplers(List<SampleResult> sampleResults) {
        sampleResults.forEach(
                it -> {
                    if (!samplersFilter.isSamplerForbidden(it)) {
                        cleanSampleData(it);
                        sampleResults.add(it);
                    }
                    List<SampleResult> subSamplers = Arrays.asList(it.getSubResults());
                    if (recordSubSamplers && subSamplers.size() > 0) {
                        addSamplers(subSamplers);
                    }
                }
        );
    }

    @Override
    public List<SampleResult> getSampleResults() {
        return allSampleResults;
    }

    protected void cleanSampleData(SampleResult sampleResult) {
        switch (recordDataLevel) {
            case "aggregate":
            case "info":
                cleanSampleRequestAndResponseData(sampleResult);
                break;
            case "error":
                if (sampleResult.getErrorCount() == 0) {
                    cleanSampleRequestAndResponseData(sampleResult);
                }
                break;
            case "debug":
                break;
            default:
                throw new IllegalArgumentException("No such record level");

        }
    }

    private void cleanSampleRequestAndResponseData(SampleResult sampleResult) {
        sampleResult.setSamplerData("");
        sampleResult.setResponseData("");
    }

}
