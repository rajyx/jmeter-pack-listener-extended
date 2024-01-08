package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;

@ExtendWith(MockitoExtension.class)
class DBSetUpperTest {

    @Mock
    private ClickHouseDataSource dataSource;

    @Mock
    private ClickHouseConnection connection;

    @Mock
    private ClickHouseProperties properties;
    @Mock
    private ClickHouseStatement statement;
    private final String DB_NAME = "db";

    @BeforeEach
    void setUp() throws SQLException {
        Mockito.when(dataSource.getConnection())
                .thenReturn(connection);
        Mockito.when(connection.createStatement())
                .thenReturn(statement);
        Mockito.when(statement.execute(Mockito.anyString()))
                .thenReturn(true);
        Mockito.when(dataSource.getProperties())
                .thenReturn(properties);
        Mockito.when(dataSource.getDatabase())
                .thenReturn(DB_NAME);
    }

    @Test
    void setUpDB_checkSetUpDBTemporaryCleanDBFromPropertiesAndCloseConnection() throws SQLException {
        DBSetUpper dbSetUpper = new DBSetUpper(dataSource);
        dbSetUpper.setUpDB();
        Mockito.verify(properties).setDatabase("");
        Mockito.verify(connection).close();
        Mockito.verify(properties).setDatabase(DB_NAME);
    }
}