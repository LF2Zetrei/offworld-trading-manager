package com.offworld;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.offworld.api.ApiClient;
import com.offworld.config.AppConfig;
import com.offworld.service.*;
import com.offworld.strategy.TradingStrategy;
import com.offworld.webhook.WebhookServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * Application entry point.
 *
 * <p>Startup sequence:
 * <ol>
 *   <li>Load configuration</li>
 *   <li>Start webhook HTTP server</li>
 *   <li>Connect SSE trade stream</li>
 *   <li>Explore galaxy</li>
 *   <li>Start ship lifecycle management</li>
 *   <li>Start automated trading strategy</li>
 * </ol>
 */
public class TradingManager {
    private static final Logger log = LoggerFactory.getLogger(TradingManager.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("=== Offworld Trading Manager starting ===");

        // ── Configuration ─────────────────────────────────────────────────────
        AppConfig config = new AppConfig();
        validateConfig(config);

        // ── Jackson ObjectMapper ──────────────────────────────────────────────
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // ── Core components ───────────────────────────────────────────────────
        ApiClient api = new ApiClient(config, mapper);
        WebhookServer webhookServer = new WebhookServer(config, mapper);

        // ── Services ──────────────────────────────────────────────────────────
        GalaxyService galaxy = new GalaxyService(api);
        MarketService market = new MarketService(api, config);
        ShipService shipService = new ShipService(api, webhookServer, config);
        ElevatorService elevator = new ElevatorService(api, config);

        // ── Strategy ─────────────────────────────────────────────────────────
        TradingStrategy strategy = new TradingStrategy(api, config, galaxy,
                market, shipService, elevator);

        // ── Shutdown hook ─────────────────────────────────────────────────────
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping...");
            webhookServer.stop();
            shutdownLatch.countDown();
        }));

        // ── Startup sequence ──────────────────────────────────────────────────
        Mono.fromRunnable(webhookServer::start)
                .then(Mono.fromRunnable(market::startTradeStream))
                .then(Mono.fromRunnable(() -> market.startPricePolling().subscribe()))
                .then(Mono.fromRunnable(() -> {
                    shipService.startWebhookListeners();
                    shipService.startPollingScheduler().subscribe();
                }))
                .then(strategy.start())
                .doOnSuccess(v -> log.info("=== Trading Manager fully started ==="))
                .doOnError(e -> {
                    log.error("Fatal startup error", e);
                    System.exit(1);
                })
                .timeout(Duration.ofMinutes(2))
                .subscribe();

        // ── Block main thread until shutdown ─────────────────────────────────
        shutdownLatch.await();
        log.info("=== Offworld Trading Manager stopped ===");
    }

    private static void validateConfig(AppConfig config) {
        if (config.getPlayerId() == null || config.getPlayerId().isBlank()) {
            log.error("player.id must be set in application.properties or PLAYER_ID env var");
            System.exit(1);
        }
        if (config.getApiKey() == null || config.getApiKey().isBlank()) {
            log.error("player.api-key must be set in application.properties or PLAYER_API_KEY env var");
            System.exit(1);
        }
        log.info("Configuration validated OK");
    }
}
