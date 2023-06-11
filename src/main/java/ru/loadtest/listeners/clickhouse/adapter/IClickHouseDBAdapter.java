package ru.loadtest.listeners.clickhouse.adapter;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

public interface IClickHouseDBAdapter {

    void prepareConnection(ClickHouseProperties properties);

    void createDatabaseIfNotExists();
}
