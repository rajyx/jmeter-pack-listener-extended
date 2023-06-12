package ru.loadtest.listeners.clickhouse.adapter;

import java.sql.SQLException;

public interface SQLFunction {
    void accept() throws SQLException;
}
