package ru.rajyx.loadtest.listeners.clickhouse.samplersbuffer;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.rajyx.loadtest.listeners.clickhouse.filter.ISamplersFilter;
import ru.rajyx.loadtest.listeners.clickhouse.filter.SamplersFilter;

import java.util.List;

public class SamplersBufferTest {
    private final int DEFAULT_SAMPLRES_QUANTITY = 5;
    private ISamplersBuffer subSamplersRecordingBuffer;
    private ISamplersBuffer noSubSamplersRecordingBuffer;
    private ISamplersFilter samplersFilter;

    @BeforeEach
    public void setUp() {
        samplersFilter = new SamplersFilter();
        samplersFilter.setFilterRegex(".*");
        subSamplersRecordingBuffer = new SamplersBuffer(samplersFilter, true);
        noSubSamplersRecordingBuffer = new SamplersBuffer(samplersFilter, false);
    }

    @Test
    public void addSamplers_checkAllSubSamplersInBufferWhenRecordSubSamplersFlagEnabled() {
        int subSamplerLevel = 3;
        SampleResult sampleResult = prepareSampleResultWithChild(subSamplerLevel);
        subSamplersRecordingBuffer.addSamplers(List.of(sampleResult));
        Assertions.assertEquals(subSamplerLevel, subSamplersRecordingBuffer.getSampleResults().size());
    }

    @Test
    public void addSamplers_checkNoSubSamplersInBufferIfRecordFlagDisabled() {
        int subSamplerLevel = 3;
        SampleResult sampleResult = prepareSampleResultWithChild(subSamplerLevel);
        noSubSamplersRecordingBuffer.addSamplers(List.of(sampleResult));
        Assertions.assertEquals(1, noSubSamplersRecordingBuffer.getSampleResults().size());
    }

    private SampleResult prepareSampleResultWithChild(int subLevel) {
        SampleResult sample = new SampleResult(System.currentTimeMillis(), 200);
        sample.setSampleLabel("sample");
        if (subLevel > 1) {
            sample.addSubResult(prepareSampleResultWithChild(subLevel - 1));
        }
        return sample;
    }
}