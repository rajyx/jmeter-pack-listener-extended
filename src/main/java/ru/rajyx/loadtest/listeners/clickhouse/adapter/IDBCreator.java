package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import java.sql.SQLException;

public interface IDBCreator {
    void setUpDB() throws SQLException;
}
