package ru.rajyx.loadtest.listeners.clickhouse;

import org.apache.jmeter.config.Arguments;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rajyx.loadtest.listeners.clickhouse.config.ClickHouseConfigV3;
import ru.rajyx.loadtest.listeners.clickhouse.config.ClickHousePluginGUIKeys;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class ClickHouseBackendListenerClientV3Test {
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
}