package ru.rajyx.loadtest.listeners.clickhouse.config;

import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ClickHouseConfigV3Test {

    @Mock
    private BackendListenerContext context;

    @Test
    public void getParameters_checkReturnsAllParamsFromStaticList() {
        Mockito.when(
                        context.getParameter(
                                ClickHousePluginGUIKeys.URL.getStringKey()
                        )
                )
                .thenReturn(
                        ClickHousePluginGUIKeys.URL.getDefaultValue()
                );
        Mockito.when(
                        context.getParameter(
                                ClickHousePluginGUIKeys.DATABASE.getStringKey()
                        )
                )
                .thenReturn(
                        ClickHousePluginGUIKeys.DATABASE.getDefaultValue()
                );
        ClickHouseConfigV3 configV3 = new ClickHouseConfigV3(context);
        ClickHouseConfigV3.parameterKeys
                .forEach(
                        key -> Mockito.verify(context).getParameter(key.getStringKey())
                );
        assertEquals(
                ClickHouseConfigV3.parameterKeys.size(),
                configV3.getParameters().size()
        );
    }
}