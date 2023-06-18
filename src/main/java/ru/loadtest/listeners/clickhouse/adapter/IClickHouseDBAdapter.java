package ru.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;
import ru.loadtest.listeners.clickhouse.config.ClickHouseConfigV3;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.List;

public interface IClickHouseDBAdapter {

    void prepareConnection(ClickHouseProperties properties);

    void createDatabaseIfNotExists(String dbName);

    void flushBatchPoints(List<SampleResult> sampleResultList, ClickHouseConfigV3 config);

    void flushAggregatedBatchPoints(List<SampleResult> sampleResultList, ClickHouseConfigV3 config);
}
