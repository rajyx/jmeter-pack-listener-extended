package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class ClickHouseBatchSenderTest {

    @Mock
    private IClickhouseDBAdapter adapter;
    private ClickHouseBatchSender batchSender;

    @BeforeEach
    public void setUp() {
        batchSender = new ClickHouseBatchSender(adapter);
    }

    @Test
    void flushBatchPoints_checkAdapterInvokeSendBatch() throws UnknownHostException, SQLException {
        List<SampleResult> sampleResults = List.of(new SampleResult());
        batchSender.flushBatchPoints(sampleResults);
        Mockito.verify(adapter).sendBatch(sampleResults);
    }

    @Test
    void flushAggregatedBatchPoints_checkAdapterInvokeSendBatch() throws UnknownHostException, SQLException {
        List<SampleResult> sampleResults = List.of(new SampleResult());
        batchSender.flushAggregatedBatchPoints(sampleResults);
        Mockito.verify(adapter).sendBatch(Mockito.anyList());
    }
}