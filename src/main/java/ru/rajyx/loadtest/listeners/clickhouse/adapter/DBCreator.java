package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DBCreator implements IDBCreator {
    private String dbUrl;

    public DBCreator(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    @Override
    public void setUpDB(ClickHouseProperties properties) throws SQLException {
        String dbName = properties.getDatabase();
        properties.setDatabase("");
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://" + dbUrl,
                properties
        );
        Connection connection = dataSource.getConnection();
        for (String query : List.of(
                "create database IF NOT EXISTS " + dbName,
                getQueryToCreateDataTable(dbName),
                getQueryToCreateStatView(dbName),
                getQueryToCreateBufferTable(dbName)
        )) {
            connection.createStatement().execute(query);
        }
        connection.close();
        properties.setDatabase(dbName);
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
}
