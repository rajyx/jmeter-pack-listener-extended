package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import java.sql.SQLException;

public interface IDBSetUpper {
    void setUpDB() throws SQLException;
}
