package com.offworld.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Application configuration loaded from application.properties or environment variables.
 * Environment variables override properties file values.
 */
public class AppConfig {
    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);
    private final Properties props = new Properties();

    public AppConfig() {
        loadFromFile();
        log.info("Configuration loaded. Server: {}, Player: {}", getServerBaseUrl(), getPlayerId());
    }

    private void loadFromFile() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
                log.debug("Loaded application.properties");
            } else {
                log.warn("application.properties not found, relying on environment variables");
            }
        } catch (IOException e) {
            log.error("Failed to load application.properties", e);
        }
    }

    public String get(String key, String defaultValue) {
        String envKey = key.toUpperCase().replace('.', '_').replace('-', '_');
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isBlank()) return envVal;
        return props.getProperty(key, defaultValue);
    }

    private int getInt(String key, int defaultValue) {
        try {
            return Integer.parseInt(get(key, String.valueOf(defaultValue)));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private double getDouble(String key, double defaultValue) {
        try {
            return Double.parseDouble(get(key, String.valueOf(defaultValue)));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    // ── Server ────────────────────────────────────────────────────────────────
    public String getServerBaseUrl() { return get("server.base-url", "http://localhost:8080"); }
    public String getPlayerId()      { return get("player.id", ""); }
    public String getApiKey()        { return get("player.api-key", ""); }

    // ── Webhook server ────────────────────────────────────────────────────────
    public String getWebhookHost()      { return get("webhook.host", "0.0.0.0"); }
    public int    getWebhookPort()      { return getInt("webhook.port", 9090); }
    public String getWebhookPublicUrl() { return get("webhook.public-url", "http://localhost:9090/webhooks"); }

    // ── Polling intervals ─────────────────────────────────────────────────────
    public int getShipPollingIntervalMs()         { return getInt("polling.ship-status-interval-ms", 2000); }
    public int getConstructionPollingIntervalMs() { return getInt("polling.construction-status-interval-ms", 5000); }
    public int getMarketRefreshIntervalMs()       { return getInt("polling.market-refresh-interval-ms", 10000); }
    public int getTradeRequestIntervalMs()        { return getInt("polling.trade-request-interval-ms", 15000); }

    // ── Trading strategy ──────────────────────────────────────────────────────
    public double getMinProfitMargin()      { return getDouble("strategy.min-profit-margin", 0.10); }
    public double getMaxOrderCreditsPct()   { return getDouble("strategy.max-order-credits-pct", 0.20); }
    public int    getMaxConcurrentShips()   { return getInt("strategy.max-concurrent-ships", 5); }
    public int    getElevatorRetryAttempts(){ return getInt("strategy.elevator-retry-attempts", 3); }
    public long   getElevatorRetryDelayMs() { return getInt("strategy.elevator-retry-delay-ms", 5000); }

    // ── HTTP timeouts ─────────────────────────────────────────────────────────
    public int getConnectTimeoutMs()  { return getInt("http.connect-timeout-ms", 5000); }
    public int getResponseTimeoutMs() { return getInt("http.response-timeout-ms", 60000); }
    public int getElevatorTimeoutMs() { return getInt("http.elevator-timeout-ms", 120000); }
}
