package ru.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class ClickHouseAdapter implements IClickHouseDBAdapter {
    private ClickHouseDataSource dataSource;
    private Connection connection;
    private String dbName;

    public ClickHouseAdapter(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public void prepareConnection(ClickHouseProperties properties) {
        dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://" + dbName,
                properties
        );
        catchConnectionExceptions(
                () -> connection = dataSource.getConnection()
        );
    }

    @Override
    public void createDatabaseIfNotExists() {
        catchConnectionExceptions(
                () -> {
                    for (String query : List.of(
                            "create database IF NOT EXISTS " + dbName,
                            getQueryToCreateDataTable(),
                            getQueryToCreateStatView(),
                            getQueryToCreateBufferTable()
                    )) {
                        connection.createStatement().execute(query);
                    }
                }
        );
    }

    private void catchConnectionExceptions(SQLFunction connectionFunction) {
        try {
            connectionFunction.accept();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flushBatchPoints(List<SampleResult> sampleResultList) {

    }

    private String getQueryToCreateDataTable() {
        return "create table IF NOT EXISTS " +
                dbName + ".jmresults_data " +
                "(ttimestamp_sec DateTime, " +
                "ttimestamp_millis UInt64, " +
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

    private String getQueryToCreateStatView() {
        return "CREATE MATERIALIZED VIEW IF NOT EXISTS " +
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

    private String getQueryToCreateBufferTable() {
        return "create table IF NOT EXISTS " +
                dbName + ".jmresults " +
                "(" +
                "ttimestamp_sec DateTime, " +
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
                "engine = Buffer(" +
                dbName + ", jmresults_data, 16, 10, 60, 10000, 100000, 1000000, 10000000)";
    }
}
