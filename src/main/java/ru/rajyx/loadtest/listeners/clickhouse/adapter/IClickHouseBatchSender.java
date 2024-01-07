package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;

public interface IClickHouseBatchSender {
    void flushBatchPoints(List<SampleResult> sampleResultList) throws UnknownHostException, SQLException;

    void flushAggregatedBatchPoints(List<SampleResult> sampleResultList) throws UnknownHostException, SQLException;
}
