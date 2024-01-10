package ru.rajyx.loadtest.listeners.clickhouse.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostUtils {
    public static String getHostname() throws UnknownHostException {
        InetAddress iAddress = InetAddress.getLocalHost();
        return iAddress.getHostName();
    }
}
