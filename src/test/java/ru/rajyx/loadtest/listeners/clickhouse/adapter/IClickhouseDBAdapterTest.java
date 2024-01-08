package ru.rajyx.loadtest.listeners.clickhouse.adapter;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class IClickhouseDBAdapterTest {

    @Mock
    private IDBCreator dbCreator;

    @Mock
    private Connection connection;
    @Mock
    private PreparedStatement point;

    @Mock
    private ClickHouseProperties properties;
    private final String PROFILE = "profile";
    private final String RUN_ID = "123";
    private final String SAMPLER_DATA = "sampler_data";
    private final String RESPONSE_DATA = "response_data";
    private int requestDataParamNumber = 11;
    private int responseDataParamNumber = 12;

    @BeforeEach
    public void setUp() throws SQLException {
        Mockito.lenient()
                .when(connection.prepareStatement(Mockito.anyString()))
                .thenReturn(point);
    }

    @Test
    void setUpDB_checkDBCreatorInvokeSetUpDBOneTime() throws SQLException {
        new ClickHouseAdapter(
                connection,
                PROFILE,
                RUN_ID,
                "info",
                dbCreator
        )
                .setUpDB(properties);
        Mockito.verify(dbCreator).setUpDB(properties);
    }

    @Test
    void sendBatch_checkAdapterSendsEmptyRequestResponseToDBWithAggregateLevel() throws UnknownHostException, SQLException {
        IClickhouseDBAdapter adapter = new ClickHouseAdapter(
                connection,
                PROFILE,
                RUN_ID,
                "aggregate",
                dbCreator
        );
        adapter.sendBatch(List.of(getPreparedSampleResult(true)));
        Mockito.verify(point).setString(requestDataParamNumber, "");
        Mockito.verify(point).setString(responseDataParamNumber, "");
        Mockito.verify(point, Mockito.times(1)).addBatch();
    }

    @Test
    void sendBatch_checkAdapterSendsEmptyRequestResponseToDBWithInfoLevel() throws UnknownHostException, SQLException {
        IClickhouseDBAdapter adapter = new ClickHouseAdapter(
                connection,
                PROFILE,
                RUN_ID,
                "info",
                dbCreator
        );
        adapter.sendBatch(List.of(getPreparedSampleResult(true)));
        Mockito.verify(point).setString(requestDataParamNumber, "");
        Mockito.verify(point).setString(responseDataParamNumber, "");
        Mockito.verify(point, Mockito.times(1)).addBatch();
    }

    @Test
    void sendBatch_checkAdapterSendsRequestResponseOfErrorSamplerIfSamplerHasOneOrMoreErrorsWithErrorLevel() throws UnknownHostException, SQLException {
        IClickhouseDBAdapter adapter = new ClickHouseAdapter(
                connection,
                PROFILE,
                RUN_ID,
                "error",
                dbCreator
        );
        adapter.sendBatch(
                List.of(
                        getPreparedSampleResult(false)
                )
        );
        Mockito.verify(point).setString(requestDataParamNumber, SAMPLER_DATA);
        Mockito.verify(point).setString(responseDataParamNumber, RESPONSE_DATA);
        Mockito.verify(point, Mockito.times(1)).addBatch();
    }

    @Test
    void sendBatch_checkAdapterSendsEmptyRequestResponseIfSamplerHasNoErrorWithErrorLevel() throws UnknownHostException, SQLException {
        IClickhouseDBAdapter adapter = new ClickHouseAdapter(
                connection,
                PROFILE,
                RUN_ID,
                "error",
                dbCreator
        );
        adapter.sendBatch(
                List.of(getPreparedSampleResult(true))
        );
        Mockito.verify(point).setString(requestDataParamNumber, "");
        Mockito.verify(point).setString(responseDataParamNumber, "");
        Mockito.verify(point, Mockito.times(1)).addBatch();
    }

    @Test
    void sendBatch_checkAdapterSendsRequestResponseWithDebugLevel() throws UnknownHostException, SQLException {
        IClickhouseDBAdapter adapter = new ClickHouseAdapter(
                connection,
                PROFILE,
                RUN_ID,
                "debug",
                dbCreator
        );
        adapter.sendBatch(
                List.of(getPreparedSampleResult(true))
        );
        Mockito.verify(point).setString(requestDataParamNumber, SAMPLER_DATA);
        Mockito.verify(point).setString(responseDataParamNumber, RESPONSE_DATA);
        Mockito.verify(point, Mockito.times(1)).addBatch();
    }

    @Test
    void sendBatch_checkWrongRecordDataLevelThrowsException() {
        IClickhouseDBAdapter adapter = new ClickHouseAdapter(
                connection,
                PROFILE,
                RUN_ID,
                "wrong_level",
                dbCreator
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.sendBatch(List.of(getPreparedSampleResult(true)))
        );
    }


    private SampleResult getPreparedSampleResult(boolean isSuccessful) {
        SampleResult sampleResult = new SampleResult(System.currentTimeMillis(), 200);
        sampleResult.setSampleLabel("sample");
        sampleResult.setThreadName("thread");
        sampleResult.setSampleCount(1);
        sampleResult.setSuccessful(isSuccessful);
        sampleResult.setResponseCodeOK();
        sampleResult.setSamplerData(SAMPLER_DATA);
        sampleResult.setResponseData(RESPONSE_DATA, "UTF-8");
        return sampleResult;
    }
}