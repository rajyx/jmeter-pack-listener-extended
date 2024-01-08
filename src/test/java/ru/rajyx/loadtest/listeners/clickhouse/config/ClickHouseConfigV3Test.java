package ru.rajyx.loadtest.listeners.clickhouse.config;

import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class ClickHouseConfigV3Test {

    @Mock
    private BackendListenerContext context;

    @Test
    public void getParameters_checkReturnsAllParamsFromStaticList() {
        ClickHouseConfigV3.parameterKeys
                .forEach(
                        key -> Mockito.when(
                                        context.getParameter(key.getStringKey())
                                )
                                .thenReturn(key.getDefaultValue())
                );
        ClickHouseConfigV3 configV3 = new ClickHouseConfigV3(context);
        ClickHouseConfigV3.parameterKeys
                .forEach(
                        key -> Mockito.verify(context).getParameter(key.getStringKey())
                );
        assertEquals(ClickHouseConfigV3.parameterKeys.size(), configV3.getParameters().size());
    }
}