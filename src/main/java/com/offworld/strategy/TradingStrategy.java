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
        // 6. Update credits every 15 seconds
        Flux.interval(Duration.ofSeconds(15))
                .flatMap(tick -> refreshPlayerProfile())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 7. Check the surface and use the elevator every 20 seconds
        Flux.interval(Duration.ofSeconds(20))
                .flatMap(tick -> manageElevatorTransfers())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // Sell station's inventory every 30 seconds
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> sellStationInventory())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        //to verify if we can ship goods
        Flux.interval(Duration.ofSeconds(45))
                .flatMap(tick -> dispatchShipToEarth())
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
        if (ships.getShipStatuses().size() >= config.getMaxConcurrentShips()) {
            log.debug("Maximum active ships reached. Waiting for current deliveries...");
            return Mono.empty();
        }

        return Flux.fromIterable(galaxy.getConnectedPlanets().values())
                .filter(p -> !p.id().equals(myPlanetId))
                .next()
                .flatMap(targetPlanet -> {
                    long buyPrice = (long) Math.ceil(opp.buyPrice());

                    log.info("Interplanetary Trade: Buying {}x {} @ {} on {}",
                            opp.quantity(), opp.goodName(), buyPrice, targetPlanet.name());

                    return market.placeBuyOrder(opp.goodName(), buyPrice, opp.quantity(), targetPlanet.id())
                            .doOnNext(o -> openOrders.put(o.id(), o))
                            .delayElement(Duration.ofSeconds(5))
                            .flatMap(order -> {
                                log.info("Hiring trucking ship to transport {} from {} to {}",
                                        opp.goodName(), targetPlanet.name(), mySystemName);
                                return ships.hireTrucking(targetPlanet.id(), myPlanetId, Map.of(opp.goodName(), opp.quantity()));
                            });
                })
                .then();
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
                    boolean hasActive = requests.stream()
                            .anyMatch(r -> "active".equals(r.status()));

                    if (!hasActive) {
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

    //Add new mission if no one from earth is buying form us
    private Mono<Void> dispatchShipToEarth() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        if (!ships.getShipStatuses().isEmpty()) {
            return Mono.empty();
        }

        return api.getStation(mySystemName, myPlanetId)
                .flatMap(station -> {
                    long ironQty = station.inventory().getOrDefault("iron_ore", 0L);

                    if (ironQty >= 500) {
                        log.info("📦 Enough iron_ore accumulated. Dispatching a cargo ship to Earth (Sol-3)!");
                        return ships.hireTrucking(myPlanetId, "Sol-3", Map.of("iron_ore", 500L)).then();
                    }
                    return Mono.empty();
                })
                .onErrorResume(e -> Mono.empty());
    }

    // for credits update
    private Mono<Void> refreshPlayerProfile() {
        return api.getPlayer()
                .doOnNext(player -> this.myCredits = player.credits())
                .then();
    }

    //for spatial elevator usage
    private Mono<Void> manageElevatorTransfers() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        return api.getSpaceElevator(mySystemName, myPlanetId)
                .flatMap(se -> {
                    if (se.warehouse() != null && config.getPlayerId().equals(se.warehouse().ownerId())) {
                        List<TransferItem> itemsToTransfer = new ArrayList<>();
                        long capacity = se.config().cabinCapacity();
                        long currentLoad = 0;

                        for (Map.Entry<String, Long> entry : se.warehouse().inventory().entrySet()) {
                            String good = entry.getKey();
                            long qty = entry.getValue();

                            if (qty > 1 && currentLoad < capacity) {
                                long availableToTransfer = qty - 1;
                                long transferQty = Math.min(availableToTransfer, capacity - currentLoad);
                                itemsToTransfer.add(new TransferItem(good, transferQty));
                                currentLoad += transferQty;
                            }
                        }

                        if (!itemsToTransfer.isEmpty()) {
                            log.info("Elevator: Transferring {} units to orbit...", currentLoad);
                            return elevator.transferToOrbit(mySystemName, myPlanetId, itemsToTransfer).then();
                        }
                    }
                    return Mono.empty();
                })
                .onErrorResume(e -> {
                    log.warn("Elevator check failed: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    //for automatic inventory sell
    private Mono<Void> sellStationInventory() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        return api.getStation(mySystemName, myPlanetId)
                .flatMap(station -> {
                    if (station.inventory() == null || station.inventory().isEmpty()) return Mono.empty();

                    return Flux.fromIterable(station.inventory().entrySet())
                            .filter(entry -> entry.getValue() > 0)
                            .filter(entry -> !"iron_ore".equals(entry.getKey()))
                            .flatMap(entry -> {
                                String good = entry.getKey();
                                long qty = entry.getValue();
                                Double currentPrice = market.getPrice(good);
                                long sellPrice = (currentPrice != null) ? Math.round(currentPrice) : 10L;

                                log.info("Selling inventory: {}x {} @ {}", qty, good, sellPrice);
                                return market.placeSellOrder(good, sellPrice, qty, myPlanetId);
                            })
                            .then();
                })
                .onErrorResume(e -> Mono.empty());
    }
}
