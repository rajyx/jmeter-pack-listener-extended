package ru.loadtest.listeners.clickhouse.samplersbuffer;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.Before;
import org.junit.Test;
import ru.loadtest.listeners.clickhouse.filter.ISamplersFilter;
import ru.loadtest.listeners.clickhouse.filter.SamplersFilter;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SamplersBufferTest {
    private final int DEFAULT_SAMPLRES_QUANTITY = 5;
    private ISamplersBuffer samplersBuffer;
    private ISamplersFilter samplersFilter;

    @Before
    public void setUp() {
        samplersFilter = new SamplersFilter();
        samplersBuffer = new SamplersBuffer(samplersFilter, true);
    }

    @Test
    public void checkNotMatchedWithRegexSamplersNotExistsInBuffer() {
        String filterRegex = "\\[HR\\].+";
        List<SampleResult> inputMatchedSamplers = getSampleResultsInQuantityWithNamePrefix(
                DEFAULT_SAMPLRES_QUANTITY,
                "[HR] http request sampler"
        );
        List<SampleResult> inputNotMatchedSamplers = getSampleResultsInQuantityWithNamePrefix(
                DEFAULT_SAMPLRES_QUANTITY,
                "[TC] transaction controller"
        );
        samplersFilter.setFilterRegex(filterRegex);
        samplersBuffer.addSamplers(inputMatchedSamplers);
        samplersBuffer.addSamplers(inputNotMatchedSamplers);
        long matchedSamplersCount = samplersBuffer.getSampleResults()
                .stream()
                .filter(sampler -> sampler.getSampleLabel().matches(filterRegex))
                .count();
        assertEquals(
                "Input matched with regex samplers quantity not equals existed samplers in buffer quantity",
                DEFAULT_SAMPLRES_QUANTITY,
                matchedSamplersCount
        );
    }

    private List<SampleResult> getSampleResultsInQuantityWithNamePrefix(int quantity, String samplerPrefix) {
        int defaultSampleElapsed = 200;
        List<SampleResult> sampleResults = new ArrayList<>();
        for (int i = 0; i < quantity; i++) {
            SampleResult sampleResult = new SampleResult(System.currentTimeMillis(), defaultSampleElapsed);
            sampleResult.setSampleLabel(samplerPrefix + i);
            sampleResult.setSuccessful(true);
            sampleResults.add(sampleResult);
        }
        return sampleResults;
    }
}