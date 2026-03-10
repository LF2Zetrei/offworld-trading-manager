package com.offworld.service;

import com.offworld.api.ApiClient;
import com.offworld.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.offworld.model.Models.*;

/**
 * Manages market observation and order management.
 *
 * <p>Subscribes to the SSE trade stream and republishes events through a hot Sink
 * so multiple consumers (strategy, logging) can subscribe independently.
 */
public class MarketService {
    private static final Logger log = LoggerFactory.getLogger(MarketService.class);

    private final ApiClient api;
    private final AppConfig config;

    // Last known prices from SSE stream
    private final Map<String, Double> lastPrices = new ConcurrentHashMap<>();

    // Hot sink for trade events — shared across subscribers
    private final Sinks.Many<TradeEvent> tradeSink =
            Sinks.many().multicast().onBackpressureBuffer(1024);

    public MarketService(ApiClient api, AppConfig config) {
        this.api = api;
        this.config = config;
    }

    /**
     * Start the SSE trade stream subscription.
     * Reconnects automatically on error with exponential backoff.
     */
    public void startTradeStream() {
        api.subscribeToTrades()
                .doOnNext(event -> {
                    lastPrices.put(event.goodName(), event.price());
                    tradeSink.tryEmitNext(event);
                })
                .doOnError(e -> log.warn("Trade stream error (will reconnect): {}", e.getMessage()))
                .retryWhen(reactor.util.retry.Retry
                        .backoff(Long.MAX_VALUE, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(30)))
                .subscribe();
        log.info("SSE trade stream subscription started");
    }

    /** Expose live trade events as a Flux (hot, shared). */
    public Flux<TradeEvent> tradeEvents() {
        return tradeSink.asFlux();
    }

    /** Periodically refresh market prices from REST. */
    public Flux<Map<String, Double>> startPricePolling() {
        return Flux.interval(Duration.ofMillis(config.getMarketRefreshIntervalMs()))
                .flatMap(tick -> api.getMarketPrices())
                .doOnNext(prices -> {
                    lastPrices.putAll(prices);
                    log.debug("Refreshed {} market prices", prices.size());
                })
                .doOnError(e -> log.error("Price polling error", e))
                .retryWhen(reactor.util.retry.Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(10)));
    }

    public Map<String, Double> getLastPrices() { return lastPrices; }

    public Double getPrice(String goodName) { return lastPrices.get(goodName); }

    /**
     * Place a limit buy order reactively.
     */
    public Mono<MarketOrder> placeBuyOrder(String goodName, long price, long quantity,
                                           String stationPlanetId) {
        PlaceOrderRequest req = new PlaceOrderRequest(goodName, "buy", "limit",
                price, quantity, stationPlanetId);
        return api.placeOrder(req)
                .doOnNext(order -> log.info("Placed buy order {} for {}x{} @ {}",
                        order.id(), quantity, goodName, price));
    }

    /**
     * Place a limit sell order reactively.
     */
    public Mono<MarketOrder> placeSellOrder(String goodName, long price, long quantity,
                                            String stationPlanetId) {
        PlaceOrderRequest req = new PlaceOrderRequest(goodName, "sell", "limit",
                price, quantity, stationPlanetId);
        return api.placeOrder(req)
                .doOnNext(order -> log.info("Placed sell order {} for {}x{} @ {}",
                        order.id(), quantity, goodName, price));
    }

    /**
     * Cancel all open/partially-filled orders — useful on shutdown or strategy reset.
     */
    public Flux<MarketOrder> cancelAllOpenOrders() {
        return api.getOrders("open")
                .flatMapMany(Flux::fromIterable)
                .mergeWith(api.getOrders("partially_filled").flatMapMany(Flux::fromIterable))
                .flatMap(order -> api.cancelOrder(order.id())
                        .doOnNext(o -> log.info("Cancelled order {}", o.id()))
                        .onErrorResume(e -> {
                            log.warn("Could not cancel order {}: {}", order.id(), e.getMessage());
                            return Mono.empty();
                        }));
    }
}
