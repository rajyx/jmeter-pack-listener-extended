package ru.rajyx.loadtest.listeners.clickhouse.utils;

import org.apache.jmeter.samplers.SampleResult;
import ru.rajyx.loadtest.listeners.clickhouse.sampler.AggregatedSampleResult;
import ru.rajyx.loadtest.listeners.clickhouse.sampler.CustomSamplerPair;

import java.util.List;
import java.util.stream.Collectors;

public class SamplersAggregator {
    private static final String DEFAULT_RESPONSE_CODE = "200";

    public static List<SampleResult> aggregateByThreadAndLabel(
            List<SampleResult> sampleResults
    ) {
        return sampleResults.stream()
                .collect(
                        Collectors.groupingBy(
                                sampler -> new CustomSamplerPair(
                                        sampler.getThreadName(),
                                        sampler.getSampleLabel()
                                ),
                                Collectors.collectingAndThen(
                                        Collectors.toList(), list -> {
                                            int errorCount = list.stream()
                                                    .mapToInt(SampleResult::getErrorCount)
                                                    .sum();
                                            int count = list.size();
                                            double average = list.stream()
                                                    .collect(
                                                            Collectors.averagingDouble(SampleResult::getTime)
                                                    );
                                            AggregatedSampleResult aggregatedSampleResult = new AggregatedSampleResult();
                                            aggregatedSampleResult.setSampleCount(count);
                                            aggregatedSampleResult.setAverageElapsedTime(average);
                                            aggregatedSampleResult.setErrorCount(errorCount);
                                            aggregatedSampleResult.setTimeStamp(System.currentTimeMillis());
                                            aggregatedSampleResult.setResponseCode(DEFAULT_RESPONSE_CODE);
                                            return aggregatedSampleResult;
                                        }
                                )
                        )
                ).entrySet()
                .stream()
                .map(
                        entry -> {
                            SampleResult sampleResult = entry.getValue();
                            sampleResult.setSampleLabel(entry.getKey().getSamplerLabel());
                            sampleResult.setThreadName(entry.getKey().getThreadName());
                            return sampleResult;
                        }
                ).collect(Collectors.toList());
    }
}
