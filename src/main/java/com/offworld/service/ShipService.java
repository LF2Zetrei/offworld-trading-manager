package com.offworld.service;

import com.offworld.api.ApiClient;
import com.offworld.api.ApiException;
import com.offworld.config.AppConfig;
import com.offworld.model.*;
import com.offworld.model.WebhookEvents;
import com.offworld.webhook.WebhookServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.offworld.model.Models.*;

/**
 * Manages the full lifecycle of ships.
 *
 * <p>Strategy: prefer webhooks for event-driven reactions; fall back to polling
 * as a safety net in case a webhook is missed.
 *
 * <p>The lifecycle:
 * <pre>
 *   in_transit_to_origin
 *     OriginDockingRequest webhook  →  PUT /dock
 *   loading
 *     (timer expires)               →  PUT /undock  (poll or ShipDocked webhook)
 *   in_transit
 *     DockingRequest webhook        →  PUT /dock
 *   unloading
 *     (timer expires)               →  PUT /undock  (poll or ShipDocked webhook)
 *   complete
 * </pre>
 */
public class ShipService {
    private static final Logger log = LoggerFactory.getLogger(ShipService.class);

    private final ApiClient api;
    private final WebhookServer webhookServer;
    private final AppConfig config;

    // Track active ships: shipId -> last known status
    private final Map<String, String> shipStatuses = new ConcurrentHashMap<>();

    public ShipService(ApiClient api, WebhookServer webhookServer, AppConfig config) {
        this.api = api;
        this.webhookServer = webhookServer;
        this.config = config;
    }

    /**
     * Start listening to webhook events and drive ship lifecycle.
     * Call once at startup.
     */
    public void startWebhookListeners() {
        // Origin docking request → authorize dock
        webhookServer.originDockingRequests()
                .flatMap(event -> authorizeDock(event.shipId())
                        .doOnError(e -> log.error("Failed to dock ship {} at origin", event.shipId(), e))
                        .onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // Destination docking request → authorize dock
        webhookServer.dockingRequests()
                .flatMap(event -> authorizeDock(event.shipId())
                        .doOnError(e -> log.error("Failed to dock ship {} at destination", event.shipId(), e))
                        .onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // ShipDocked → wait for loading/unloading to finish, then undock
        webhookServer.shipDockedEvents()
                .flatMap(event -> scheduleUndock(event.shipId(), event.status())
                        .doOnError(e -> log.error("Failed to undock ship {}", event.shipId(), e))
                        .onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // Ship complete → log and remove from tracking
        webhookServer.shipCompleteEvents()
                .doOnNext(event -> {
                    shipStatuses.remove(event.shipId());
                    log.info("Ship {} delivery complete", event.shipId());
                })
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        log.info("Ship webhook listeners started");
    }

    /**
     * Safety-net polling: periodically scan all active ships and advance
     * any that are stuck waiting for authorization.
     */
    public Flux<Ship> startPollingScheduler() {
        return Flux.interval(Duration.ofMillis(config.getShipPollingIntervalMs()))
                .flatMap(tick -> api.getShips()
                        .flatMapMany(Flux::fromIterable)
                        .filter(ship -> isActionable(ship.status()))
                        .flatMap(ship -> advanceShip(ship)
                                .onErrorResume(e -> {
                                    log.warn("Error advancing ship {}: {}", ship.id(), e.getMessage());
                                    return Mono.empty();
                                })))
                .doOnError(e -> log.error("Ship polling error", e))
                .retryWhen(reactor.util.retry.Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(10)));
    }

    private boolean isActionable(String status) {
        return status.equals("awaiting_origin_docking_auth") ||
               status.equals("awaiting_origin_undocking_auth") ||
               status.equals("awaiting_docking_auth") ||
               status.equals("awaiting_undocking_auth");
    }

    private Mono<Ship> advanceShip(Ship ship) {
        return switch (ship.status()) {
            case "awaiting_origin_docking_auth", "awaiting_docking_auth" ->
                    authorizeDock(ship.id());
            case "awaiting_origin_undocking_auth", "awaiting_undocking_auth" ->
                    authorizeUndock(ship.id());
            default -> Mono.just(ship);
        };
    }

    private Mono<Ship> authorizeDock(String shipId) {
        log.info("Authorizing dock for ship {}", shipId);
        return api.authorizedock(shipId)
                .doOnNext(ship -> shipStatuses.put(shipId, ship.status()))
                .onErrorResume(e -> {
                    if (e.getMessage() != null && (e.getMessage().contains("403") || e.getMessage().contains("409"))) {
                        return Mono.empty();
                    }
                    return Mono.error(e);
                })
                .retryWhen(reactor.util.retry.Retry.fixedDelay(2, Duration.ofSeconds(3))
                        .filter(e -> e.getMessage() != null && e.getMessage().contains("503")));
    }

    private Mono<Ship> scheduleUndock(String shipId, String dockedStatus) {
        // Poll ship status until loading/unloading timer expires, then undock
        return Flux.interval(Duration.ofMillis(config.getShipPollingIntervalMs()))
                .flatMap(tick -> api.getShip(shipId))
                .filter(ship -> ship.status().startsWith("awaiting") &&
                               (ship.status().contains("undocking")))
                .next()
                .timeout(Duration.ofMinutes(10))
                .flatMap(ship -> authorizeUndock(shipId))
                .doOnError(e -> log.warn("scheduleUndock timeout/error for {}", shipId));
    }

    public Mono<Ship> authorizeUndock(String shipId) {
        log.info("Authorizing undock for ship {}", shipId);
        return api.authorizeUndock(shipId)
                .doOnNext(ship -> shipStatuses.put(shipId, ship.status()))
                .onErrorResume(e -> {
                    if (e.getMessage() != null && (e.getMessage().contains("403") || e.getMessage().contains("409"))) {
                        return Mono.empty();
                    }
                    return Mono.error(e);
                })
                .retryWhen(reactor.util.retry.Retry.fixedDelay(2, Duration.ofSeconds(3))
                        .filter(e -> e.getMessage() != null && e.getMessage().contains("503")));
    }

    /**
     * Hire a trucking ship and register it for tracking.
     */
    public Mono<Ship> hireTrucking(String originPlanetId, String destPlanetId,
                                    Map<String, Long> cargo) {
        TruckingRequest req = new TruckingRequest(originPlanetId, destPlanetId, cargo);
        return api.hireTrucking(req)
                .doOnNext(ship -> {
                    shipStatuses.put(ship.id(), ship.status());
                    log.info("Hired trucking ship {} from {} to {}", ship.id(), originPlanetId, destPlanetId);
                });
    }

    public Map<String, String> getShipStatuses() { return shipStatuses; }
}
