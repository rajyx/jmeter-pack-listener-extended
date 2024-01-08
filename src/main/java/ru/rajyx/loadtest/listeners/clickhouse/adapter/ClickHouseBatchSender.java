package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;
import ru.rajyx.loadtest.listeners.clickhouse.utils.SamplersAggregator;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;

public class ClickHouseBatchSender implements IClickHouseBatchSender {
    private final IClickhouseDBAdapter clickhouseAdapter;

    public ClickHouseBatchSender(IClickhouseDBAdapter clickhouseAdapter) {
        this.clickhouseAdapter = clickhouseAdapter;
    }

    @Override
    public void flushBatchPoints(List<SampleResult> sampleResultList) throws UnknownHostException, SQLException {
        clickhouseAdapter.sendBatch(sampleResultList);
    }

    @Override
    public void flushAggregatedBatchPoints(List<SampleResult> sampleResultList) throws UnknownHostException, SQLException {
        List<SampleResult> aggregatedSampleResults = SamplersAggregator.aggregateByThreadAndLabel(sampleResultList);
        clickhouseAdapter.sendBatch(aggregatedSampleResults);
    }
}
