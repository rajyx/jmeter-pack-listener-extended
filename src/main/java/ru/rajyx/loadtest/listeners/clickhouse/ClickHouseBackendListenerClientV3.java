package ru.rajyx.loadtest.listeners.clickhouse;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rajyx.loadtest.listeners.clickhouse.adapter.*;
import ru.rajyx.loadtest.listeners.clickhouse.config.ClickHouseConfigV3;
import ru.rajyx.loadtest.listeners.clickhouse.config.ClickHousePluginGUIKeys;
import ru.rajyx.loadtest.listeners.clickhouse.filter.ISamplersFilter;
import ru.rajyx.loadtest.listeners.clickhouse.filter.SamplersFilter;
import ru.rajyx.loadtest.listeners.clickhouse.samplersbuffer.ISamplersBuffer;
import ru.rajyx.loadtest.listeners.clickhouse.samplersbuffer.SamplersBuffer;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClickHouseBackendListenerClientV3 extends AbstractBackendListenerClient implements Runnable {
    protected ClickHouseConfigV3 clickHouseConfig;
    protected IClickHouseBatchSender clickHouseBatchSender;
    protected IClickhouseDBAdapter clickhouseDBAdapter;
    protected IDBSetUpper dbCreator;

    protected ISamplersBuffer samplersBuffer;
    private ScheduledExecutorService scheduler;
    private final Logger LOGGER = LoggerFactory.getLogger(ClickHouseBackendListenerClientV3.class);

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
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
        samplersBuffer = new SamplersBuffer(
                getSamplersFilter(),
                Boolean.parseBoolean(
                        clickHouseConfig.getParameters()
                                .get(ClickHousePluginGUIKeys.RECORD_SUB_SAMPLERS.getStringKey())
                )
        );
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        LOGGER.info("Shutting down clickHouse scheduler...");
        String recordDataLevel = clickHouseConfig.getParameters().get(ClickHousePluginGUIKeys.RECORD_DATA_LEVEL.getStringKey());
        if (recordDataLevel.equals("aggregate")) {
            clickHouseBatchSender.flushAggregatedBatchPoints(samplersBuffer.getSampleResults());
            samplersBuffer.clearBuffer();
        } else {
            clickHouseBatchSender.flushBatchPoints(samplersBuffer.getSampleResults());
            samplersBuffer.clearBuffer();
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
        try {
            if (
                    recordDataLevel.equals("aggregate")
                            && samplersBuffer.getSampleResults().size() >= groupByCount * batchSize
            ) {
                clickHouseBatchSender.flushAggregatedBatchPoints(samplersBuffer.getSampleResults());
                samplersBuffer.clearBuffer();
            } else if (samplersBuffer.getSampleResults().size() >= batchSize) {
                clickHouseBatchSender.flushBatchPoints(samplersBuffer.getSampleResults());
                samplersBuffer.clearBuffer();
            }
        } catch (UnknownHostException | SQLException e) {
            sendStackTraceToLogs(e);
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
        String dbUrl = configParameters.get(ClickHousePluginGUIKeys.URL.getStringKey());
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://" + dbUrl,
                properties
        );
        dbCreator = new DBSetUpper(clickHouseDataSource);
        try {
            clickhouseDBAdapter = new ClickHouseAdapter(
                    clickHouseDataSource.getConnection(),
                    configParameters.get(ClickHousePluginGUIKeys.PROFILE_NAME.getStringKey()),
                    configParameters.get(ClickHousePluginGUIKeys.RUN_ID.getStringKey()),
                    configParameters.get(ClickHousePluginGUIKeys.RECORD_DATA_LEVEL.getStringKey()),
                    dbCreator
            );
            boolean createDefinition = Boolean.parseBoolean(
                    configParameters.get(
                            ClickHousePluginGUIKeys.CREATE_DEFINITIONS.getStringKey()
                    )
            );
            if (createDefinition) {
                clickhouseDBAdapter.setUpDB();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        clickHouseBatchSender = new ClickHouseBatchSender(clickhouseDBAdapter);
    }

    private ISamplersFilter getSamplersFilter() {
        boolean useRegexToSamplersFilter = Boolean.parseBoolean(
                clickHouseConfig
                        .getParameters()
                        .get(ClickHousePluginGUIKeys.USE_REGEX_FOR_SAMPLERS_LIST.getStringKey())
        );
        String samplersList = clickHouseConfig
                .getParameters()
                .get(ClickHousePluginGUIKeys.SAMPLERS_LIST.getStringKey());
        final String SAMPLERS_SEPARATOR = ";";
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

    private void sendStackTraceToLogs(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        LOGGER.error(sw.toString());
    }

    @Override
    public void run() {
    }
}
