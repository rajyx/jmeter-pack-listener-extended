package ru.loadtest.listeners.clickhouse.samplersbuffer;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.Test;
import ru.loadtest.listeners.clickhouse.filter.ISamplersFilter;
import ru.loadtest.listeners.clickhouse.filter.SamplersFilter;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class SamplersBufferTest {

    @Test
    public void checkAddSamplersUsingSimpleRegex() {
        String filterRegex = ".*";
        boolean recordSubSamplers = true;
        ISamplersFilter samplersFilter = new SamplersFilter()
                .setFilterRegex(filterRegex);
        SamplersBuffer samplersBuffer = new SamplersBuffer(
                samplersFilter,
                recordSubSamplers
        );
        List<SampleResult> sampleResults = getSampleResults(10);
        samplersBuffer.addSamplers(sampleResults);
        assertTrue(
                "Input Samplers count not equals buffers samplers count",
                sampleResults.size() == samplersBuffer.getSampleResults().size()
        );
    }

    @Test
    public void getSampleResults() {
    }

    private List<SampleResult> getSampleResults(int quantity) {
        List<SampleResult> sampleResults = new ArrayList<>();
        for (int i = 0; i < quantity; i++) {
            SampleResult sampleResult = new SampleResult(System.currentTimeMillis(), 200);
            sampleResult.setSampleLabel("Samper " + i);
            sampleResult.setSuccessful(true);
            sampleResults.add(sampleResult);
        }
        return sampleResults;
    }
}