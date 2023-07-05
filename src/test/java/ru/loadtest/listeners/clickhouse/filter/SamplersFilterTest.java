package ru.loadtest.listeners.clickhouse.filter;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class SamplersFilterTest {

    private ISamplersFilter samplersFilter;

    @Before
    public void setUp() {
        samplersFilter = new SamplersFilter();
    }

    @Test
    public void checkIsSamplerValidWhenItMathesWithFilterRegex() {
        String filterRegex = "^V.+";
        SampleResult validSampleResult = getSampleResultWithSampleLabel("Valid sample result");
        SampleResult notValidSampleResult = getSampleResultWithSampleLabel("Not valid sample result");
        samplersFilter.setFilterRegex(filterRegex);
        assertTrue(samplersFilter.isSamplerValid(validSampleResult));
        assertFalse(samplersFilter.isSamplerValid(notValidSampleResult));
    }

    @Test
    public void checkIsSamplerValidTrueWhenItContainsInSamplersToStore() {
        String oneOfSamlersToStoreLabels = "Sample result";
        Set<String> samplersToStore = Set.of(
                oneOfSamlersToStoreLabels,
                "Child sample result"
        );
        samplersFilter.setSamplersToStore(samplersToStore);
        SampleResult validSampleResult = getSampleResultWithSampleLabel(oneOfSamlersToStoreLabels);
        SampleResult notValidSampleResult = getSampleResultWithSampleLabel("Not valid sample result");
        assertTrue(samplersFilter.isSamplerValid(validSampleResult));
        assertFalse(samplersFilter.isSamplerValid(notValidSampleResult));
    }

    private SampleResult getSampleResultWithSampleLabel(String sampleLabel) {
        final int defaultSampleElapsed = 200;
        SampleResult sampleResult = new SampleResult(System.currentTimeMillis(), defaultSampleElapsed);
        sampleResult.setSampleLabel(sampleLabel);
        return sampleResult;
    }
}