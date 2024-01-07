package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;

public interface IClickhouseDBAdapter {
    void setUpDB(ClickHouseProperties properties) throws SQLException;

    void sendBatch(List<SampleResult> sampleResults) throws SQLException, UnknownHostException;
}
