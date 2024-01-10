package ru.rajyx.loadtest.listeners.clickhouse.utils;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SamplersAggregatorTest {


    @Test
    void aggregateByThreadAndLabel_checkSuccessfulSamplersAggregationHasCorrectSampleCountAndAverageTime() {
        final int sampleQuantityPerThread = 10;
        List<String> threadNames = List.of("firstThread", "secondThread");
        List<SampleResult> sampleResults = prepareSampleResults(
                true,
                sampleQuantityPerThread,
                threadNames
        );
        List<SampleResult> aggregatedSampleResults = SamplersAggregator
                .aggregateByThreadAndLabel(sampleResults);
        assertEquals(threadNames.size(), aggregatedSampleResults.size());
        aggregatedSampleResults.forEach(
                sample -> {
                    double averageElapsedTime = sampleResults.stream()
                            .filter(
                                    sampleResult -> sampleResult.getThreadName()
                                            .equals(sample.getThreadName())
                            )
                            .collect(Collectors.averagingDouble(SampleResult::getTime));
                    assertEquals((long) averageElapsedTime, sample.getTime());
                    assertEquals(sampleQuantityPerThread, sample.getSampleCount());
                    assertEquals(0, sample.getErrorCount());
                }
        );
    }

    @Test
    public void aggregateByThreadAndLabel_checkFailedSamplersAggregationHasCorrectErrorCount() {
        final int sampleQuantityPerThread = 10;
        List<String> threadNames = List.of("firstThread", "secondThread");
        List<SampleResult> sampleResults = prepareSampleResults(
                false,
                sampleQuantityPerThread,
                threadNames
        );
        List<SampleResult> aggregatedSampleResults = SamplersAggregator
                .aggregateByThreadAndLabel(sampleResults);
        aggregatedSampleResults.forEach(
                sampleResult -> {
                    assertEquals(sampleQuantityPerThread, sampleResult.getErrorCount());
                    assertEquals(sampleQuantityPerThread, sampleResult.getSampleCount());
                }
        );
    }

    private List<SampleResult> prepareSampleResults(
            boolean isSuccessful,
            int sampleQuantityPerThread,
            List<String> threadNames
    ) {
        final String sampleLabel = "sample";
        Random random = new Random();
        return threadNames.stream()
                .flatMap(
                        threadName -> Stream.generate(
                                        () -> {
                                            SampleResult sampleResult = new SampleResult(
                                                    System.currentTimeMillis(),
                                                    random.nextInt(1000)
                                            );
                                            sampleResult.setThreadName(threadName);
                                            sampleResult.setSampleLabel(sampleLabel);
                                            sampleResult.setSuccessful(isSuccessful);
                                            return sampleResult;
                                        }
                                )
                                .limit(sampleQuantityPerThread)
                )
                .collect(Collectors.toList());
    }
}