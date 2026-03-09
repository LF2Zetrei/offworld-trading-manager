package com.offworld.strategy;

import com.offworld.api.ApiClient;
import com.offworld.config.AppConfig;
import com.offworld.model.*;
import com.offworld.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import com.offworld.model.Models.*;

/**
 * Automated trading strategy.
 *
 * <p>Pipeline overview:
 * <ol>
 *   <li>Observe SSE trade events to detect price trends.</li>
 *   <li>Periodically scan order books for spread opportunities.</li>
 *   <li>Place limit buy + sell orders to capture the spread.</li>
 *   <li>Monitor active ships count; hire trucking for physical cargo moves.</li>
 *   <li>Manage trade requests to seed supply/demand as needed.</li>
 * </ol>
 */
public class TradingStrategy {
    private static final Logger log = LoggerFactory.getLogger(TradingStrategy.class);

    private final ApiClient api;
    private final AppConfig config;
    private final GalaxyService galaxy;
    private final MarketService market;
    private final ShipService ships;
    private final ElevatorService elevator;

    // Track recent prices per good from SSE
    private final Map<String, Deque<Double>> priceHistory = new ConcurrentHashMap<>();
    // Track our own open orders: orderId -> order
    private final Map<String, MarketOrder> openOrders = new ConcurrentHashMap<>();
    // Active trucking ship count
    private final AtomicInteger activeShips = new AtomicInteger(0);

    // The planet ID of our main station
    private String myPlanetId;
    private String mySystemName;
    private long myCredits;

    public TradingStrategy(ApiClient api, AppConfig config, GalaxyService galaxy,
                           MarketService market, ShipService ships, ElevatorService elevator) {
        this.api = api;
        this.config = config;
        this.galaxy = galaxy;
        this.market = market;
        this.ships = ships;
        this.elevator = elevator;
    }

    /**
     * Bootstrap: find our station, then launch all automated loops.
     */
    public Mono<Void> start() {
        return initializeMyStation()
                .then(Mono.fromRunnable(this::launchPipelines));
    }

    // Remplace initializeMyStation() par ceci temporairement
    private Mono<Void> initializeMyStation() {
        return api.getPlayer()
                .flatMap(player -> {
                    myCredits = player.credits();
                    log.info("Player: {} | Credits: {}", player.name(), myCredits);
                    return api.updatePlayer(new UpdatePlayerRequest(null, config.getWebhookPublicUrl()));
                })
                .then(galaxy.exploreGalaxy()
                        .doOnNext(p -> {
                            // LOG TEMPORAIRE pour voir ce qu'on reçoit
                            Station st = p.station();
                            log.info("Planet: {} | Station: {} | OwnerId: {}",
                                    p.name(),
                                    st != null ? st.name() : "NULL",
                                    st != null ? st.ownerId() : "NULL");
                        })
                        .filter(p -> {
                            Station st = p.station();
                            return st != null && config.getPlayerId().equals(st.ownerId());
                        })
                        .next()
                        .switchIfEmpty(Mono.error(new IllegalStateException("No owned station found!")))
                        .doOnNext(planet -> {
                            myPlanetId = planet.id();
                            mySystemName = galaxy.getSystemForPlanet(planet.id());
                            log.info("My station: {} on planet {} in system {}",
                                    planet.station().name(), mySystemName);
                        }))
                .then();
    }

    private void launchPipelines() {
        // 1. React to SSE trade events
        market.tradeEvents()
                .doOnNext(this::onTradeEvent)
                .subscribeOn(Schedulers.parallel())
                .subscribe();

        // 2. Periodic market scan & order placement
        Flux.interval(Duration.ofMillis(config.getMarketRefreshIntervalMs()))
                .flatMap(tick -> scanAndTrade())
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(e -> log.error("Market scan error", e))
                .retryWhen(reactor.util.retry.Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(15)))
                .subscribe();

        // 3. Monitor our orders
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> refreshOpenOrders())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 4. Seed trade requests to generate supply
        Flux.interval(Duration.ofMillis(config.getTradeRequestIntervalMs()))
                .flatMap(tick -> manageTradeRequests())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 5. Leaderboard check
        Flux.interval(Duration.ofMinutes(1))
                .flatMap(tick -> api.getLeaderboard())
                .doOnNext(board -> board.stream()
                        .filter(e -> e.playerId().equals(config.getPlayerId()))
                        .findFirst()
                        .ifPresent(e -> log.info("Leaderboard: rank={}, profit={}",
                                board.indexOf(e) + 1, e.profit())))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        log.info("All trading pipelines launched");
    }

    // ─── SSE reaction ─────────────────────────────────────────────────────────

    private void onTradeEvent(TradeEvent event) {
        priceHistory.computeIfAbsent(event.goodName(), k -> new ArrayDeque<>(20))
                .offer(event.price());
        // Keep only last 20 data points
        Deque<Double> hist = priceHistory.get(event.goodName());
        while (hist.size() > 20) hist.poll();
    }

    // ─── Market scan ──────────────────────────────────────────────────────────

    private Mono<Void> scanAndTrade() {
        return api.getMarketPrices()
                .flatMap(prices -> {
                    if (prices.isEmpty()) return Mono.empty();

                    // Find goods with a favorable spread by checking order books in parallel
                    List<String> goods = new ArrayList<>(prices.keySet());
                    return Flux.fromIterable(goods)
                            .flatMap(good -> api.getOrderBook(good)
                                    .map(book -> evaluateSpread(good, book))
                                    .onErrorResume(e -> Mono.empty()), 5)  // max 5 parallel
                            .filter(opp -> opp != null)
                            .sort(Comparator.comparingDouble(Opportunity::estimatedProfit).reversed())
                            .next()
                            .flatMap(opp -> executeOpportunity(opp));
                })
                .then();
    }

    private record Opportunity(String goodName, double buyPrice, double sellPrice,
                                long quantity, double estimatedProfit) {}

    private Opportunity evaluateSpread(String goodName, OrderBook book) {
        if (book.asks() == null || book.asks().isEmpty()) return null;
        if (book.bids() == null || book.bids().isEmpty()) return null;

        double bestAsk = book.asks().get(0).price();       // cheapest seller
        double bestBid = book.bids().get(0).price();       // highest buyer

        double spread = bestBid - bestAsk;
        double marginPct = bestAsk > 0 ? spread / bestAsk : 0;

        if (marginPct < config.getMinProfitMargin()) return null;

        long availableQty = Math.min(book.asks().get(0).totalQuantity(),
                                    book.bids().get(0).totalQuantity());
        long maxAffordable = (long)(myCredits * config.getMaxOrderCreditsPct() / bestAsk);
        long qty = Math.min(availableQty, Math.max(1, maxAffordable));

        double estimatedProfit = qty * spread * 0.9; // 90% fill assumption
        return new Opportunity(goodName, bestAsk, bestBid, qty, estimatedProfit);
    }

    private Mono<Void> executeOpportunity(Opportunity opp) {
        log.info("Trading opportunity: {} — buy@{} sell@{} qty={} est.profit={}",
                opp.goodName(), opp.buyPrice(), opp.sellPrice(), opp.quantity(),
                String.format("%.0f", opp.estimatedProfit()));

        // Place buy order
        Mono<MarketOrder> buy = market.placeBuyOrder(
                opp.goodName(), opp.buyPrice(), opp.quantity(), myPlanetId)
                .doOnNext(o -> openOrders.put(o.id(), o));

        // Place sell order after buy is confirmed
        Mono<MarketOrder> sell = market.placeSellOrder(
                opp.goodName(), opp.sellPrice(), opp.quantity(), myPlanetId)
                .doOnNext(o -> openOrders.put(o.id(), o));

        return buy.then(sell).then();
    }

    // ─── Order monitoring ─────────────────────────────────────────────────────

    private Mono<Void> refreshOpenOrders() {
        return api.getOrders()
                .flatMapMany(Flux::fromIterable)
                .doOnNext(order -> {
                    openOrders.put(order.id(), order);
                    if ("filled".equals(order.status()) || "cancelled".equals(order.status())) {
                        log.debug("Order {} is {}", order.id(), order.status());
                        openOrders.remove(order.id());
                    }
                })
                .then();
    }

    // ─── Trade requests ───────────────────────────────────────────────────────

    private Mono<Void> manageTradeRequests() {
        return api.getTradeRequests()
                .flatMap(requests -> {
                    long activeCount = requests.stream()
                            .filter(r -> "active".equals(r.status())).count();

                    if (activeCount == 0) {
                        // Seed a standing export request to keep supply flowing
                        log.info("No active trade requests, seeding export...");
                        TradeRequestCreate req = new TradeRequestCreate(
                                myPlanetId, "iron_ore", "export",
                                "standing", 5, null, null);
                        return api.createTradeRequest(req)
                                .doOnNext(r -> log.info("Created trade request {}", r.id()))
                                .then();
                    }
                    return Mono.empty();
                });
    }
}
