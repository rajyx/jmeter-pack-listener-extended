package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class IClickhouseDBAdapterTest {

    @Mock
    private IDBCreator dbCreator;

    @Mock
    private Connection connection;

    @Mock
    private ClickHouseProperties properties;

    private IClickhouseDBAdapter adapter;

    @BeforeEach
    public void setUp() {
        adapter = new ClickHouseAdapter(
                connection,
                "profile",
                "123",
                "info",
                dbCreator
        );
        System.out.println(connection);
    }

    @Test
    void setUpDB_checkDBCreatorInvokeSetUpDBOneTime() throws SQLException {
        adapter.setUpDB(properties);
        Mockito.verify(dbCreator).setUpDB(properties);
    }

    @Test
    void sendBatch() {
        assertEquals(1, 1);
    }
}