package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;
import ru.rajyx.loadtest.listeners.clickhouse.utils.HostUtils;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class ClickHouseAdapter implements IClickhouseDBAdapter {
    private Connection connection;
    private String profileName;
    private String runId;
    private String recordDataLevel;
    private IDBCreator dbCreator;

    public ClickHouseAdapter(
            Connection connection,
            String profileName,
            String runId,
            String recordDataLevel,
            IDBCreator dbCreator
    ) {
        this.connection = connection;
        this.profileName = profileName;
        this.runId = runId;
        this.recordDataLevel = recordDataLevel;
        this.dbCreator = dbCreator;
    }

    @Override
    public void setUpDB(ClickHouseProperties properties) throws SQLException {
        dbCreator.setUpDB(properties);
    }

    @Override
    public void sendBatch(List<SampleResult> sampleResults) throws SQLException, UnknownHostException {
        PreparedStatement point = connection.prepareStatement(getQueryForFlushBatchPoint());
        for (SampleResult sampleResult : sampleResults) {
            point.setTimestamp(1, new Timestamp(sampleResult.getTimeStamp()));
            point.setLong(2, sampleResult.getTimeStamp());
            point.setString(3, profileName);
            point.setString(4, runId);
            point.setString(5, HostUtils.getHostname());
            point.setString(6, sampleResult.getThreadName());
            point.setString(7, sampleResult.getSampleLabel());
            point.setInt(8, sampleResult.getSampleCount());
            point.setInt(9, sampleResult.getErrorCount());
            point.setDouble(10, sampleResult.getTime());
            setFilteredRequestResponseData(point, sampleResult);
            point.setString(13, sampleResult.getResponseCode());
            point.addBatch();
        }
        point.executeBatch();
    }


    private void setFilteredRequestResponseData(
            PreparedStatement point,
            SampleResult sampleResult
    ) throws SQLException {
        String samplerRequest = sampleResult.getSamplerData();
        String samplerResponse = sampleResult.getResponseDataAsString();
        switch (recordDataLevel) {
            case "aggregate":
            case "info":
                samplerRequest = "";
                samplerResponse = "";
                break;
            case "error":
                if (sampleResult.getErrorCount() == 0) {
                    samplerRequest = "";
                    samplerResponse = "";
                }
                break;
            case "debug":
                break;
            default:
                throw new IllegalArgumentException("No such record level");
        }
        point.setString(11, samplerRequest);
        point.setString(12, samplerResponse);
    }

    private String getQueryForFlushBatchPoint() {
        return "INSERT INTO jmresults (" +
                "timestamp_sec, " +
                "timestamp_millis, " +
                "profile_name, " +
                "run_id, " +
                "hostname, " +
                "thread_name, " +
                "sample_label, " +
                "points_count, " +
                "errors_count, " +
                "average_time, " +
                "request, " +
                "response, " +
                "res_code" +
                ")" +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
    }
}
