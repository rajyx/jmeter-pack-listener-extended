package ru.loadtest.listeners.clickhouse.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClickHouseConfigV3 {
    private Map<String, String> parameters;
    public static final List<ClickHousePluginGUIKeys> parameterKeys = List.of(
            ClickHousePluginGUIKeys.PROFILE_NAME,
            ClickHousePluginGUIKeys.RUN_ID,
            ClickHousePluginGUIKeys.URL,
            ClickHousePluginGUIKeys.USER,
            ClickHousePluginGUIKeys.PASSWORD,
            ClickHousePluginGUIKeys.DATABASE,
            ClickHousePluginGUIKeys.SAMPLERS_LIST,
            ClickHousePluginGUIKeys.USE_REGEX_FOR_SAMPLERS_LIST,
            ClickHousePluginGUIKeys.RECORD_SUB_SAMPLERS,
            ClickHousePluginGUIKeys.GROUP_BY_COUNT,
            ClickHousePluginGUIKeys.BATCH_SIZE,
            ClickHousePluginGUIKeys.RECORD_DATA_LEVEL,
            ClickHousePluginGUIKeys.CREATE_DEFINITIONS,
            ClickHousePluginGUIKeys.CONNECT_TIMEOUT
    );

    public ClickHouseConfigV3(BackendListenerContext context) {
        this.parameters = new HashMap<>();
        parameterKeys.forEach(
                it -> {
                    String keyStringValue = it.getStringKey();
                    String contextParameterValue = context.getParameter(keyStringValue);
                    if (
                            (it.equals(ClickHousePluginGUIKeys.URL)
                                    || it.equals(ClickHousePluginGUIKeys.DATABASE))
                                    && StringUtils.isEmpty(contextParameterValue)
                    ) {
                        throw new IllegalArgumentException(keyStringValue + " must not be empty");
                    }
                    parameters.put(keyStringValue, context.getParameter(keyStringValue));
                }
        );
    }

    public Map<String, String> getParameters() {
        return parameters;
    }
}
