package ru.rajyx.loadtest.listeners.clickhouse.adapter;


import org.apache.jmeter.samplers.SampleResult;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

public class ClickHouseAdapter implements IClickHouseDBAdapter {
    private ClickHouseDataSource dataSource;
    private Connection connection;
    private String dbUrl;
    private boolean createDefinitions;
    private String profileName;
    private String runId;
    private String recordDataLevel;

    public ClickHouseAdapter(
            String dbUrl,
            boolean createDefinitions,
            String profileName,
            String runId,
            String recordDataLevel
    ) {
        this.dbUrl = dbUrl;
        this.createDefinitions = createDefinitions;
        this.profileName = profileName;
        this.runId = runId;
        this.recordDataLevel = recordDataLevel;
    }

    @Override
    public void prepareConnection(ClickHouseProperties properties) {
        if (createDefinitions) {
            setUpDatabase(properties);
        }
        dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://" + dbUrl,
                properties
        );
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void setUpDatabase(ClickHouseProperties properties) {
        String dbName = properties.getDatabase();
        properties.setDatabase("");
        dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://" + dbUrl,
                properties
        );
        try {
            connection = dataSource.getConnection();
            createDatabaseIfNotExists(dbName);
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            properties.setDatabase(dbName);
        }
    }

    private void createDatabaseIfNotExists(String dbName) {
        try {
            for (String query : List.of(
                    "create database IF NOT EXISTS " + dbName,
                    getQueryToCreateDataTable(dbName),
                    getQueryToCreateStatView(dbName),
                    getQueryToCreateBufferTable(dbName)
            )) {
                connection.createStatement().execute(query);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flushBatchPoints(List<SampleResult> sampleResultList) {
        try {
            PreparedStatement point = connection.prepareStatement(getQueryForFlushBatchPoint());
            for (SampleResult sampleResult : sampleResultList) {
                point.setTimestamp(1, new Timestamp(sampleResult.getTimeStamp()));
                point.setLong(2, sampleResult.getTimeStamp());
                point.setString(3, profileName);
                point.setString(4, runId);
                point.setString(5, getHostname());
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
        } catch (SQLException | UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flushAggregatedBatchPoints(List<SampleResult> sampleResultList) {
        try {
            PreparedStatement point = connection.prepareStatement(getQueryForFlushBatchPoint());
            List<AggregatedSampeResult> aggregatedSampleResults = sampleResultList.stream()
                    .collect(
                            Collectors.groupingBy(
                                    sampler -> new CustomSamplerPair(sampler.getThreadName(), sampler.getSampleLabel()),
                                    Collectors.collectingAndThen(Collectors.toList(), list -> {
                                                int errorsCount = list.stream().mapToInt(SampleResult::getErrorCount).sum();
                                                int count = list.size();
                                                double average = list.stream().collect(Collectors.averagingDouble(SampleResult::getTime));
                                                return new AggregatedSampeResult()
                                                        .setPointCount(count)
                                                        .setAverageTime((long) average)
                                                        .setErrorsCount(errorsCount);
                                            }
                                    )
                            )
                    ).entrySet()
                    .stream()
                    .map(
                            entry -> entry.getValue()
                                    .setSampleLabel(entry.getKey().getSamplerLabel())
                                    .setThreadName(entry.getKey().getThreadName())
                    ).collect(Collectors.toList());
            for (AggregatedSampeResult sampeResult : aggregatedSampleResults) {
                long currentTimeInMillis = System.currentTimeMillis();
                point.setTimestamp(1, new Timestamp(currentTimeInMillis));
                point.setLong(2, currentTimeInMillis);
                point.setString(3, profileName);
                point.setString(4, runId);
                point.setString(5, getHostname());
                point.setString(6, sampeResult.getThreadName());
                point.setString(7, sampeResult.getSampleLabel());
                point.setInt(8, sampeResult.getPointCount());
                point.setInt(9, sampeResult.getErrorsCount());
                point.setDouble(10, sampeResult.getAverageTime());
                point.setString(11, "");
                point.setString(12, "");
                point.setString(13, "");
                point.addBatch();
            }
            point.executeBatch();
        } catch (SQLException | UnknownHostException e) {
            e.printStackTrace();
        }

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

    private String getQueryToCreateDataTable(String dbName) {
        return "create table IF NOT EXISTS " +
                dbName + ".jmresults_data " +
                "(timestamp_sec DateTime, " +
                "timestamp_millis UInt64, " +
                "profile_name LowCardinality(String), " +
                "run_id LowCardinality(String), " +
                "hostname LowCardinality(String), " +
                "thread_name LowCardinality(String), " +
                "sample_label LowCardinality(String), " +
                "points_count UInt64, " +
                "errors_count UInt64, " +
                "average_time Float64, " +
                "request String, " +
                "response String, " +
                "res_code LowCardinality(String)) " +
                "engine = MergeTree " +
                "PARTITION BY toYYYYMMDD(timestamp_sec) " +
                "ORDER BY (timestamp_sec) " +
                "TTL timestamp_sec + INTERVAL 1 WEEK " +
                "SETTINGS index_granularity = 8192";
    }

    private String getQueryToCreateStatView(String dbName) {
        return "CREATE VIEW IF NOT EXISTS " +
                dbName + ".jmresults_statistic (" +
                "`timestamp_sec` DateTime, " +
                "`timestamp_millis` UInt64, " +
                "`profile_name` LowCardinality(String), " +
                "`run_id` LowCardinality(String), " +
                "`thread_name` LowCardinality(String), " +
                "`hostname` String Codec(LZ4), " +
                "`sample_label` LowCardinality(String), " +
                "`points_count` UInt64, " +
                "`errors_count` UInt64, " +
                "`average_time` Float64, " +
                "`res_code` LowCardinality(String)) " +
                "AS " +
                "SELECT timestamp_sec, " +
                "timestamp_millis, " +
                "profile_name, " +
                "run_id, " +
                "thread_name, " +
                "hostname, " +
                "sample_label, " +
                "points_count, " +
                "errors_count, " +
                "average_time, " +
                "res_code " +
                "FROM " + dbName + ".jmresults_data";
    }

    private String getQueryToCreateBufferTable(String dbName) {
        return "create table IF NOT EXISTS " +
                dbName + ".jmresults " +
                "(" +
                "timestamp_sec DateTime, " +
                "timestamp_millis UInt64, " +
                "profile_name LowCardinality(String), " +
                "run_id LowCardinality(String), " +
                "hostname LowCardinality(String), " +
                "thread_name LowCardinality(String), " +
                "sample_label LowCardinality(String), " +
                "points_count UInt64, " +
                "errors_count UInt64, " +
                "average_time Float64, " +
                "request String, " +
                "response String, " +
                "res_code LowCardinality(String) " +
                ") " +
                "engine = Buffer('" +
                dbName + "', 'jmresults_data', 16, 10, 60, 10000, 100000, 1000000, 10000000)";
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

    private String getHostname() throws UnknownHostException {
        InetAddress iAddress = InetAddress.getLocalHost();
        String hostName = iAddress.getHostName();
        return hostName;
    }
}
