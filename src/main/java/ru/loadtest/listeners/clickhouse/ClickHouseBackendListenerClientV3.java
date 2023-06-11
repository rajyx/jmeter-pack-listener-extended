package ru.loadtest.listeners.clickhouse;

import cloud.testload.jmeter.ClickHouseBackendListenerClientV2;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import ru.loadtest.listeners.clickhouse.adapter.ClickHouseAdapter;
import ru.loadtest.listeners.clickhouse.adapter.IClickHouseDBAdapter;
import ru.loadtest.listeners.clickhouse.config.ClickHouseConfigV3;
import ru.loadtest.listeners.clickhouse.config.ClickHousePluginGUIKeys;
import ru.loadtest.listeners.clickhouse.filter.ISamplersFilter;
import ru.loadtest.listeners.clickhouse.filter.SamplersFilter;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClickHouseBackendListenerClientV3 extends ClickHouseBackendListenerClientV2 {
    protected ClickHouseConfigV3 clickHouseConfig;
    protected IClickHouseDBAdapter clickHouseDBAdapter;
    protected ISamplersFilter samplersFilter;
    private String SAMPLERS_SEPARATOR = ";";

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        ClickHouseConfigV3.parameterKeys.forEach(
                it -> arguments.addArgument(it.getStringKey(), it.getDefaultValue())
        );
        return arguments;
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        clickHouseConfig = new ClickHouseConfigV3(context);
        setupClickHouseAdapter();
        if (
                Boolean.parseBoolean(
                        clickHouseConfig.getParameters().get(ClickHousePluginGUIKeys.CREATE_DEFINITIONS.getStringKey())
                )
        ) {
            clickHouseDBAdapter.createDatabaseIfNotExists();
        }
        setUpSamplersFilter();
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
        // Indicates whether to write sub sample records to the database
        recordSubSamples = Boolean.parseBoolean(context.getParameter(KEY_RECORD_SUB_SAMPLES, "false"));
    }

    private void setupClickHouseAdapter() {
        Map<String, String> configParameters = clickHouseConfig.getParameters();
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setCompress(true);
        properties.setDatabase(configParameters.get(ClickHousePluginGUIKeys.DATABASE.getStringKey()));
        properties.setUser(configParameters.get(ClickHousePluginGUIKeys.USER.getStringKey()));
        properties.setPassword(configParameters.get(ClickHousePluginGUIKeys.PASSWORD.getStringKey()));
        properties.setConnectionTimeout(Integer.parseInt(configParameters.get(ClickHousePluginGUIKeys.CONNECT_TIMEOUT.getStringKey())));
        properties.setSocketTimeout(Integer.parseInt(configParameters.get(ClickHousePluginGUIKeys.CONNECT_TIMEOUT.getStringKey())));
        clickHouseDBAdapter = new ClickHouseAdapter(configParameters.get(ClickHousePluginGUIKeys.URL.getStringKey()));
        clickHouseDBAdapter.prepareConnection(properties);
    }

    private void setUpSamplersFilter() {
        Boolean useRegexToSamplersFilter = Boolean.parseBoolean(
                clickHouseConfig
                        .getParameters()
                        .get(ClickHousePluginGUIKeys.USE_REGEX_FOR_SAMPLERS_LIST.getStringKey())
        );
        String samplersList = clickHouseConfig
                .getParameters()
                .get(ClickHousePluginGUIKeys.SAMPLERS_LIST.getStringKey());
        samplersFilter = new SamplersFilter()
                .setFilterRegex(samplersList)
                .setSamplersToStore(
                        useRegexToSamplersFilter
                                ? Arrays.stream(
                                samplersList.split(SAMPLERS_SEPARATOR)
                        ).collect(Collectors.toSet())
                                : null
                );
    }
}
