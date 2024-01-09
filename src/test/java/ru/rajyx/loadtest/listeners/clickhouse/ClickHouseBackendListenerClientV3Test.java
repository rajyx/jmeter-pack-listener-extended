package ru.rajyx.loadtest.listeners.clickhouse;

import org.apache.jmeter.config.Arguments;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import ru.rajyx.loadtest.listeners.clickhouse.adapter.IClickHouseBatchSender;
import ru.rajyx.loadtest.listeners.clickhouse.adapter.IClickhouseDBAdapter;
import ru.rajyx.loadtest.listeners.clickhouse.adapter.IDBSetUpper;
import ru.rajyx.loadtest.listeners.clickhouse.config.ClickHouseConfigV3;
import ru.rajyx.loadtest.listeners.clickhouse.config.ClickHousePluginGUIKeys;
import ru.rajyx.loadtest.listeners.clickhouse.samplersbuffer.ISamplersBuffer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class ClickHouseBackendListenerClientV3Test {
    @Mock
    private ClickHouseConfigV3 configV3;
    @Mock
    private IClickHouseBatchSender clickHouseBatchSender;
    @Mock
    private IClickhouseDBAdapter clickhouseDBAdapter;
    @Mock
    private IDBSetUpper dbCreator;
    @Mock
    private ISamplersBuffer samplersBuffer;
    @Mock
    private ScheduledExecutorService scheduler;
    @Mock
    private Logger logger;
    private ClickHouseBackendListenerClientV3 listener;

    @BeforeEach
    void setUp() {
        listener = new ClickHouseBackendListenerClientV3();
    }

    @Test
    void getDefaultParameters_checkReturnsArgumentsWithAllConfigKeys() {
        Arguments defaultParameters = listener.getDefaultParameters();
        List<ClickHousePluginGUIKeys> configKeys = ClickHouseConfigV3.parameterKeys;
        Map<String, String> defaultParamsAsMap = defaultParameters.getArgumentsAsMap();
        configKeys.stream()
                .collect(
                        Collectors.toMap(
                                ClickHousePluginGUIKeys::getStringKey,
                                ClickHousePluginGUIKeys::getDefaultValue
                        )
                )
                .forEach(
                        (key, value) -> assertEquals(value, defaultParamsAsMap.get(key))
                );
        assertEquals(configKeys.size(), defaultParamsAsMap.size());
    }

    @Test
    void setupTest() {
    }

    @Test
    void teardownTest() {
    }

    @Test
    void handleSampleResults() {
    }
}