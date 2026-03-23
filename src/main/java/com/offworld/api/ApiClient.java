package com.offworld.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.offworld.config.AppConfig;
import com.offworld.model.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import com.offworld.model.Models.*;

/**
 * Reactive HTTP client wrapping all server REST endpoints.
 *
 * <p>All methods return Mono/Flux — nothing blocks a thread. The space-elevator
 * transfer endpoint uses a dedicated long-timeout client because the server holds
 * the connection open for the duration of the transfer.
 */
public class ApiClient {
    private static final Logger log = LoggerFactory.getLogger(ApiClient.class);

    private final AppConfig config;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final HttpClient elevatorClient;   // long-timeout variant

    public ApiClient(AppConfig config, ObjectMapper mapper) {
        this.config = config;
        this.mapper = mapper;

        ConnectionProvider provider = ConnectionProvider.builder("offworld-pool")
                .maxConnections(50)
                .pendingAcquireMaxCount(100)
                .build();

        this.httpClient = HttpClient.create(provider)
                .baseUrl(config.getServerBaseUrl())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMs())
                .responseTimeout(Duration.ofMillis(config.getResponseTimeoutMs()))
                .headers(h -> h.set("Authorization", "Bearer " + config.getApiKey())
                               .set("Content-Type", "application/json")
                               .set("Accept", "application/json"));

        // Space elevator may block for transfer_duration_secs — use a much longer timeout
        this.elevatorClient = HttpClient.create(provider)
                .baseUrl(config.getServerBaseUrl())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMs())
                .responseTimeout(Duration.ofMillis(config.getElevatorTimeoutMs()))
                .headers(h -> h.set("Authorization", "Bearer " + config.getApiKey())
                               .set("Content-Type", "application/json")
                               .set("Accept", "application/json"));
    }

    // ─── Body helper ─────────────────────────────────────────────────────────────

    private Publisher<ByteBuf> toBody(Object body) {
        return Mono.fromCallable(() -> {
            byte[] bytes = mapper.writeValueAsBytes(body);
            return io.netty.buffer.Unpooled.wrappedBuffer(bytes);
        });
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private <T> Mono<T> get(String path, TypeReference<T> type) {
        return httpClient.get()
                .uri(path)
                .responseSingle((res, body) -> body.asString()
                        .flatMap(json -> {
                            if (res.status().code() >= 400) {
                                log.warn("GET {} => {} : {}", path, res.status().code(), json);
                                return Mono.error(new ApiException(res.status().code(), json));
                            }
                            return Mono.fromCallable(() -> mapper.readValue(json, type));
                        }));
    }

    private <T> Mono<T> post(String path, Object body, TypeReference<T> type) {
        return httpClient.post()
                .uri(path)
                .send(toBody(body))
                .responseSingle((res, buf) -> buf.asString()
                        .flatMap(resp -> {
                            if (res.status().code() >= 400) {
                                log.warn("POST {} => {} : {}", path, res.status().code(), resp);
                                return Mono.error(new ApiException(res.status().code(), resp));
                            }
                            return Mono.fromCallable(() -> mapper.readValue(resp, type));
                        }));
    }

    private <T> Mono<T> put(String path, Object body, TypeReference<T> type) {
        return httpClient.put()
                .uri(path)
                .send(toBody(body))
                .responseSingle((res, buf) -> buf.asString()
                        .flatMap(resp -> {
                            if (res.status().code() >= 400) {
                                log.warn("PUT {} => {} : {}", path, res.status().code(), resp);
                                return Mono.error(new ApiException(res.status().code(), resp));
                            }
                            return Mono.fromCallable(() -> mapper.readValue(resp, type));
                        }));
    }

    private <T> Mono<T> delete(String path, TypeReference<T> type) {
        return httpClient.delete()
                .uri(path)
                .responseSingle((res, body) -> body.asString()
                        .flatMap(json -> {
                            if (res.status().code() >= 400) {
                                log.warn("DELETE {} => {} : {}", path, res.status().code(), json);
                                return Mono.error(new ApiException(res.status().code(), json));
                            }
                            return Mono.fromCallable(() -> mapper.readValue(json, type));
                        }));
    }

    // ─── Galaxy ───────────────────────────────────────────────────────────────

    public Mono<List<StarSystem>> getSystems() {
        return get("/systems", new TypeReference<>() {});
    }

    public Mono<StarSystem> getSystem(String name) {
        return get("/systems/" + encode(name), new TypeReference<>() {});
    }

    public Mono<List<Planet>> getPlanets(String systemName) {
        return get("/systems/" + encode(systemName) + "/planets", new TypeReference<>() {});
    }

    public Mono<Planet> getPlanet(String systemName, String planetId) {
        return get("/systems/" + encode(systemName) + "/planets/" + encode(planetId), new TypeReference<>() {});
    }

    public Mono<List<Planet>> getSettlements(String systemName) {
        return get("/settlements/" + encode(systemName), new TypeReference<>() {});
    }

    public Mono<Models.Settlement> getSettlement(String systemName, String planetId) {
        return get("/settlements/" + encode(systemName) + "/" + encode(planetId), new TypeReference<>() {});
    }

    // ─── Station ──────────────────────────────────────────────────────────────

    public Mono<Models.Station> getStation(String systemName, String planetId) {
        return get("/settlements/" + encode(systemName) + "/" + encode(planetId) + "/station",
                   new TypeReference<>() {});
    }

    // ─── Space Elevator ───────────────────────────────────────────────────────

    public Mono<SpaceElevator> getSpaceElevator(String systemName, String planetId) {
        return get("/settlements/" + encode(systemName) + "/" + encode(planetId) + "/space-elevator",
                   new TypeReference<>() {});
    }


    /**
     * Blocking transfer — the server holds the HTTP connection open.
     * Uses the long-timeout elevatorClient so a reactor thread is not blocked.
     * The Mono is subscribed on a bounded elastic scheduler by the caller.
     */
    public Mono<TransferResult> transferElevator(String systemName, String planetId,
                                                 TransferRequest request) {
        return elevatorClient.post()
                .uri("/settlements/" + encode(systemName) + "/" + encode(planetId) + "/space-elevator/transfer")
                .send(toBody(request))
                .responseSingle((res, buf) -> buf.asString()
                        .flatMap(resp -> {
                            if (res.status().code() >= 400) {
                                return Mono.error(new ApiException(res.status().code(), resp));
                            }
                            return Mono.fromCallable(() -> mapper.readValue(resp, TransferResult.class));
                        }));
    }

    // ─── Market ───────────────────────────────────────────────────────────────

    public Mono<MarketOrder> placeOrder(PlaceOrderRequest request) {
        return post("/market/orders", request, new TypeReference<>() {});
    }

    public Mono<List<MarketOrder>> getOrders() {
        return get("/market/orders", new TypeReference<>() {});
    }

    public Mono<List<MarketOrder>> getOrders(String status) {
        return get("/market/orders?status=" + status, new TypeReference<>() {});
    }

    public Mono<MarketOrder> getOrder(String orderId) {
        return get("/market/orders/" + orderId, new TypeReference<>() {});
    }

    public Mono<MarketOrder> cancelOrder(String orderId) {
        return delete("/market/orders/" + orderId, new TypeReference<>() {});
    }

    public Mono<OrderBook> getOrderBook(String goodName) {
        return get("/market/book/" + goodName, new TypeReference<>() {});
    }

    public Mono<Map<String, Double>> getMarketPrices() {
        return get("/market/prices", new TypeReference<>() {});
    }

    /**
     * SSE stream of trade events. Returns a Flux that never completes (infinite stream).
     * Backpressure is handled by the caller using onBackpressureDrop or buffer operators.
     */
    public Flux<TradeEvent> subscribeToTrades() {
        return httpClient.get()
                .uri("/market/trades")
                .responseContent()
                .asString()
                .filter(line -> line.startsWith("data:"))
                .map(line -> line.substring(5).trim())
                .filter(json -> !json.isBlank())
                .flatMap(json -> {
                    try {
                        return Flux.just(mapper.readValue(json, TradeEvent.class));
                    } catch (Exception e) {
                        log.warn("Failed to parse trade event: {}", json, e);
                        return Flux.empty();
                    }
                })
                .onBackpressureDrop(dropped -> log.debug("Dropped trade event due to backpressure: {}", dropped.id()))
                .doOnError(e -> log.error("SSE trade stream error", e));
    }

    // ─── Shipping ─────────────────────────────────────────────────────────────

    public Mono<Ship> hireTrucking(TruckingRequest request) {
        return post("/trucking", request, new TypeReference<>() {});
    }

    public Mono<List<Ship>> getShips() {
        return get("/ships", new TypeReference<>() {});
    }

    public Mono<List<Ship>> getShips(String status) {
        return get("/ships?status=" + status, new TypeReference<>() {});
    }

    public Mono<Ship> getShip(String shipId) {
        return get("/ships/" + shipId, new TypeReference<>() {});
    }

    public Mono<Ship> authorizedock(String shipId) {
        return put("/ships/" + shipId + "/dock", Map.of("authorized", true), new TypeReference<>() {});
    }

    public Mono<Ship> authorizeUndock(String shipId) {
        return put("/ships/" + shipId + "/undock", Map.of("authorized", true), new TypeReference<>() {});
    }

    // ─── Trade Requests ───────────────────────────────────────────────────────

    public Mono<TradeRequest> createTradeRequest(TradeRequestCreate request) {
        return post("/trade", request, new TypeReference<>() {});
    }

    public Mono<List<TradeRequest>> getTradeRequests() {
        return get("/trade", new TypeReference<>() {});
    }

    public Mono<TradeRequest> getTradeRequest(String id) {
        return get("/trade/" + id, new TypeReference<>() {});
    }

    public Mono<TradeRequest> cancelTradeRequest(String id) {
        return delete("/trade/" + id, new TypeReference<>() {});
    }

    // ─── Construction ─────────────────────────────────────────────────────────

    public Mono<List<ConstructionProject>> getConstructionProjects() {
        return get("/construction", new TypeReference<>() {});
    }

    public Mono<ConstructionProject> getConstructionProject(String id) {
        return get("/construction/" + id, new TypeReference<>() {});
    }

    public Mono<ConstructionProject> upgradeStation(UpgradeStationRequest request) {
        return post("/construction/upgrade-station", request, new TypeReference<>() {});
    }

    public Mono<ConstructionProject> upgradeElevator(String planetId) {
        return post("/construction/upgrade-elevator", Map.of("planet_id", planetId), new TypeReference<>() {});
    }
    public Mono<ConstructionProject> installStation(Map<String, String> request) {
        return post("/construction/install-station", request, new TypeReference<>() {});
    }
    public Mono<ConstructionProject> foundSettlement(Map<String, Object> request) {
        return post("/construction/found-settlement", request, new TypeReference<>() {});
    }

    // ─── Player ───────────────────────────────────────────────────────────────

    public Mono<Player> getPlayer() {
        return get("/players/" + config.getPlayerId(), new TypeReference<>() {});
    }

    public Mono<Player> updatePlayer(UpdatePlayerRequest request) {
        return put("/players/" + config.getPlayerId(), request, new TypeReference<>() {});
    }

    // ─── Leaderboard ─────────────────────────────────────────────────────────

    public Mono<List<LeaderboardEntry>> getLeaderboard() {
        return get("/leaderboard", new TypeReference<>() {});
    }

    // ─── Util ────────────────────────────────────────────────────────────────

    private static String encode(String s) {
        return s.replace(" ", "%20");
    }
}
