package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;

public interface IDBCreator {
    void setUpDB(ClickHouseProperties properties) throws SQLException;
}
