package ru.loadtest.listeners.clickhouse;

import cloud.testload.jmeter.ClickHouseBackendListenerClientV2;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import ru.loadtest.listeners.clickhouse.adapter.ClickHouseAdapter;
import ru.loadtest.listeners.clickhouse.adapter.IClickHouseDBAdapter;
import ru.loadtest.listeners.clickhouse.config.ClickHouseConfigV3;
import ru.loadtest.listeners.clickhouse.config.ClickHousePluginGUIKeys;
import ru.loadtest.listeners.clickhouse.filter.ISamplersFilter;
import ru.loadtest.listeners.clickhouse.filter.SamplersFilter;
import ru.loadtest.listeners.clickhouse.samplersbuffer.ISamplersBuffer;
import ru.loadtest.listeners.clickhouse.samplersbuffer.SamplersBuffer;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClickHouseBackendListenerClientV3 extends ClickHouseBackendListenerClientV2 {
    protected ClickHouseConfigV3 clickHouseConfig;
    protected IClickHouseDBAdapter clickHouseDBAdapter;

    protected ISamplersBuffer samplersBuffer;
    private String SAMPLERS_SEPARATOR = ";";
    private ScheduledExecutorService scheduler;

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
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
        samplersBuffer = new SamplersBuffer(
                getSamplersFilter(),
                clickHouseConfig.getParameters()
                        .get(ClickHousePluginGUIKeys.RECORD_DATA_LEVEL.getStringKey()),
                Boolean.parseBoolean(
                        clickHouseConfig.getParameters()
                                .get(ClickHousePluginGUIKeys.RECORD_SUB_SAMPLERS.getStringKey())
                )
        );
    }

    public void teardownTest(BackendListenerContext context) throws Exception {
        super.getLogger().info("Shutting down clickHouse scheduler...");
        String recordDataLevel = clickHouseConfig.getParameters().get(ClickHousePluginGUIKeys.RECORD_DATA_LEVEL.getStringKey());
        if (recordDataLevel.equals("aggregate")) {
            clickHouseDBAdapter.flushAggregatedBatchPoints(samplersBuffer.getSampleResults(), clickHouseConfig);
        } else {
            clickHouseDBAdapter.flushBatchPoints(samplersBuffer.getSampleResults(), clickHouseConfig);
        }

        super.teardownTest(context);
    }

    @Override
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        samplersBuffer.addSamplers(sampleResults);
        Map<String, String> configParameters = clickHouseConfig.getParameters();
        String recordDataLevel = configParameters.get(ClickHousePluginGUIKeys.RECORD_DATA_LEVEL.getStringKey());
        int groupByCount = Integer.parseInt(configParameters.get(ClickHousePluginGUIKeys.GROUP_BY_COUNT.getStringKey()));
        int batchSize = Integer.parseInt(configParameters.get(ClickHousePluginGUIKeys.BATCH_SIZE.getStringKey()));
        if (
                recordDataLevel.equals("aggregate")
                        && samplersBuffer.getSampleResults().size() >= groupByCount * batchSize
        ) {
            clickHouseDBAdapter.flushAggregatedBatchPoints(samplersBuffer.getSampleResults(), clickHouseConfig);
        } else if (samplersBuffer.getSampleResults().size() >= batchSize) {
            clickHouseDBAdapter.flushBatchPoints(samplersBuffer.getSampleResults(), clickHouseConfig);
        }
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

    private ISamplersFilter getSamplersFilter() {
        Boolean useRegexToSamplersFilter = Boolean.parseBoolean(
                clickHouseConfig
                        .getParameters()
                        .get(ClickHousePluginGUIKeys.USE_REGEX_FOR_SAMPLERS_LIST.getStringKey())
        );
        String samplersList = clickHouseConfig
                .getParameters()
                .get(ClickHousePluginGUIKeys.SAMPLERS_LIST.getStringKey());
        return new SamplersFilter()
                .setFilterRegex(samplersList)
                .setSamplersToStore(
                        useRegexToSamplersFilter
                                ? null
                                : Arrays.stream(
                                samplersList.split(SAMPLERS_SEPARATOR)
                        ).collect(Collectors.toSet())
                );
    }
}
