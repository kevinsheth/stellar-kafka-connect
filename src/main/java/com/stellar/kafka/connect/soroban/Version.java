package com.stellar.kafka.connect.soroban;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

final class Version {
    private static final String PATH = "/stellar-soroban-source-connector-version.properties";
    private static final String VERSION = loadVersion();

    private Version() {
    }

    static String getVersion() {
        return VERSION;
    }

    private static String loadVersion() {
        try (InputStream stream = Version.class.getResourceAsStream(PATH)) {
            if (stream == null) {
                return "unknown";
            }
            Properties properties = new Properties();
            properties.load(stream);
            return properties.getProperty("version", "unknown").trim();
        } catch (IOException e) {
            return "unknown";
        }
    }
}
