package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;

public interface IClickhouseDBAdapter {
    void setUpDB() throws SQLException;

    void sendBatch(List<SampleResult> sampleResults) throws SQLException, UnknownHostException;
}
