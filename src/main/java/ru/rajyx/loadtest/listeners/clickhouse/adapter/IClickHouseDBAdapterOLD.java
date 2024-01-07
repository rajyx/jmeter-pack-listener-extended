package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.util.List;

public interface IClickHouseDBAdapterOLD {

    void prepareConnection(ClickHouseProperties properties);


    void flushBatchPoints(List<SampleResult> sampleResultList);

    void flushAggregatedBatchPoints(List<SampleResult> sampleResultList);
}
