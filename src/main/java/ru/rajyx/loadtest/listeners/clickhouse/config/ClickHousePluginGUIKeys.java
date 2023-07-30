package ru.rajyx.loadtest.listeners.clickhouse.config;

public enum ClickHousePluginGUIKeys {
    PROFILE_NAME("profileName", "TEST"),
    RUN_ID("runId", "0"),
    URL("chUrl", "localhost:8123"),
    USER("chUser", "default"),
    PASSWORD("chPassword", "passwd123"),
    DATABASE("chDatabase", "default"),
    SAMPLERS_LIST("samplersList", ".*"),
    USE_REGEX_FOR_SAMPLERS_LIST("useRegexForSamplersList", "true"),
    RECORD_SUB_SAMPLERS("recordSubSamples", "true"),
    GROUP_BY_COUNT("groupByCount", "100"),
    BATCH_SIZE("batchSize", "1000"),
    RECORD_DATA_LEVEL("recordDataLevel", "error"),
    CREATE_DEFINITIONS("createDefinitions", "true"),
    CONNECT_TIMEOUT("connectTimeout", "60000");

    private String stringKey;
    private String defaultValue;

    ClickHousePluginGUIKeys(String keyString, String defaultValue) {
        this.stringKey = keyString;
        this.defaultValue = defaultValue;
    }

    public String getStringKey() {
        return this.stringKey;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}
