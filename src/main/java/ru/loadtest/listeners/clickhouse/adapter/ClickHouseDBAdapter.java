package ru.loadtest.listeners.clickhouse.adapter;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

public interface ClickHouseDBAdapter {

    void prepareConnection(ClickHouseProperties properties);

    void createDatabaseIfNotExists();
}
