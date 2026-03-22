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
 * Pipeline overview:
 * 1. SSE trade events for price trends.
 * 2. Market scan for spread opportunities.
 * 3. Limit orders placement.
 * 4. Internal logistics & trucking.
 * 5. Goal evaluation & resource reservation.
 */
public class TradingStrategy {
    private static final Logger log = LoggerFactory.getLogger(TradingStrategy.class);

    private final ApiClient api;
    private final AppConfig config;
    private final GalaxyService galaxy;
    private final MarketService market;
    private final ShipService ships;
    private final ElevatorService elevator;

    private final Map<String, Deque<Double>> priceHistory = new ConcurrentHashMap<>();
    private final Map<String, MarketOrder> openOrders = new ConcurrentHashMap<>();
    private final AtomicInteger activeShips = new AtomicInteger(0);

    private String myPlanetId;
    private String mySystemName;
    private long myCredits;

    // Reservation System (Step 1)
    private Goal currentGoal = null;
    private final Map<String, Long> reservedGoods = new ConcurrentHashMap<>();
    private long reservedCredits = 0;

    private record Goal(String type, String targetPlanetId, long requiredCredits, Map<String, Long> requiredGoods) {}

    public TradingStrategy(ApiClient api, AppConfig config, GalaxyService galaxy,
                           MarketService market, ShipService ships, ElevatorService elevator) {
        this.api = api;
        this.config = config;
        this.galaxy = galaxy;
        this.market = market;
        this.ships = ships;
        this.elevator = elevator;
    }

    public Mono<Void> start() {
        return initializeMyStation()
                .then(Mono.fromRunnable(this::launchPipelines));
    }

    private Mono<Void> initializeMyStation() {
        return api.getPlayer()
                .flatMap(player -> {
                    myCredits = player.credits();
                    log.info("Player: {} | Credits: {}", player.name(), myCredits);
                    return api.updatePlayer(new UpdatePlayerRequest(null, config.getWebhookPublicUrl()));
                })
                .then(galaxy.exploreGalaxy()
                        .doOnNext(p -> {
                            Station st = p.station();
                            log.info("Planet discovered: {} | Station: {} | Owner: {}",
                                    p.name(),
                                    st != null ? st.name() : "NONE",
                                    st != null ? st.ownerId() : "NONE");
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
                            log.info("My station confirmed: {} on planet {} in system {}",
                                    planet.station().name(), myPlanetId, mySystemName);
                        }))
                .then();
    }

    private void launchPipelines() {
        // 1. SSE trade events
        market.tradeEvents()
                .doOnNext(this::onTradeEvent)
                .subscribeOn(Schedulers.parallel())
                .subscribe();

        // 2. Periodic market scan
        Flux.interval(Duration.ofMillis(config.getMarketRefreshIntervalMs()))
                .flatMap(tick -> scanAndTrade())
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(e -> log.error("Market scan error", e))
                .retryWhen(reactor.util.retry.Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(15)))
                .subscribe();

        // 3. Monitor orders
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> refreshOpenOrders())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 4. Trade requests (Economy seeding)
        Flux.interval(Duration.ofMillis(config.getTradeRequestIntervalMs()))
                .flatMap(tick -> manageTradeRequests())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 5. Goal evaluation & Reservations
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> evaluateGoals())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 6. Sell station inventory (Respecting reservations)
        Flux.interval(Duration.ofSeconds(35))
                .flatMap(tick -> sellStationInventory())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 7. Internal Logistics
        Flux.interval(Duration.ofSeconds(45))
                .flatMap(tick -> manageInternalLogistics())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 8. Elevator transfers
        Flux.interval(Duration.ofSeconds(20))
                .flatMap(tick -> manageElevatorTransfers())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 9. Profile & Leaderboard refresh
        Flux.interval(Duration.ofSeconds(15))
                .flatMap(tick -> refreshPlayerProfile())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        // 10. Status Report every 3 minutes
        Flux.interval(Duration.ofMinutes(3))
                .flatMap(tick -> printStationStatusReport())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        Flux.interval(Duration.ofMinutes(1))
                .flatMap(tick -> api.getLeaderboard())
                .doOnNext(board -> board.stream()
                        .filter(e -> e.playerId().equals(config.getPlayerId()))
                        .findFirst()
                        .ifPresent(e -> log.info("Leaderboard: Rank={}, Profit={}",
                                board.indexOf(e) + 1, e.profit())))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        log.info("All trading pipelines successfully launched.");
    }

    private void onTradeEvent(TradeEvent event) {
        priceHistory.computeIfAbsent(event.goodName(), k -> new ArrayDeque<>(20))
                .offer(event.price());
        Deque<Double> hist = priceHistory.get(event.goodName());
        while (hist.size() > 20) hist.poll();
    }

    private Mono<Void> scanAndTrade() {
        return api.getMarketPrices()
                .flatMap(prices -> {
                    if (prices.isEmpty()) return Mono.empty();
                    List<String> goods = new ArrayList<>(prices.keySet());
                    return Flux.fromIterable(goods)
                            .flatMap(good -> api.getOrderBook(good)
                                    .map(book -> evaluateSpread(good, book))
                                    .onErrorResume(e -> Mono.empty()), 5)
                            .filter(Objects::nonNull)
                            .sort(Comparator.comparingDouble(Opportunity::estimatedProfit).reversed())
                            .next()
                            .flatMap(this::executeOpportunity);
                })
                .then();
    }

    private record Opportunity(String goodName, double buyPrice, double sellPrice,
                               long quantity, double estimatedProfit) {}

    private Opportunity evaluateSpread(String goodName, OrderBook book) {
        if (book.asks() == null || book.asks().isEmpty() || book.bids() == null || book.bids().isEmpty()) return null;

        double bestAsk = book.asks().get(0).price();
        double bestBid = book.bids().get(0).price();
        double spread = bestBid - bestAsk;
        double marginPct = bestAsk > 0 ? spread / bestAsk : 0;

        if (marginPct < config.getMinProfitMargin()) return null;

        long availableQty = Math.min(book.asks().get(0).totalQuantity(), book.bids().get(0).totalQuantity());
        long maxAffordable = (long)((myCredits - reservedCredits) * config.getMaxOrderCreditsPct() / bestAsk);
        long qty = Math.min(availableQty, Math.max(1, maxAffordable));

        if (qty <= 0) return null;

        double estimatedProfit = qty * spread * 0.9;
        return new Opportunity(goodName, bestAsk, bestBid, qty, estimatedProfit);
    }

    private Mono<Void> executeOpportunity(Opportunity opp) {
        if (ships.getShipStatuses().size() >= config.getMaxConcurrentShips()) {
            log.debug("Ship limit reached. Skipping trade opportunity.");
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
                                log.info("Hiring trucker for {} from {} to {}",
                                        opp.goodName(), targetPlanet.name(), myPlanetId);
                                return ships.hireTrucking(targetPlanet.id(), myPlanetId, Map.of(opp.goodName(), opp.quantity()));
                            });
                })
                .then();
    }

    private Mono<Void> refreshOpenOrders() {
        return api.getOrders()
                .flatMapMany(Flux::fromIterable)
                .doOnNext(order -> {
                    openOrders.put(order.id(), order);
                    if ("filled".equals(order.status()) || "cancelled".equals(order.status())) {
                        openOrders.remove(order.id());
                    }
                })
                .then();
    }

    /**
     * DYNAMIC ECONOMY SEEDING
     * Creates an IMPORT request to force the planet's economy to produce goods and put them in our warehouse.
     */
    private Mono<Void> manageTradeRequests() {
        return api.getTradeRequests()
                .flatMap(requests -> {
                    boolean hasActive = requests.stream().anyMatch(r -> "active".equals(r.status()));
                    if (hasActive) return Mono.empty();

                    Map<String, Double> prices = market.getLastPrices();
                    String bestGood = "water";

                    if (!prices.isEmpty()) {
                        bestGood = prices.entrySet().stream()
                                .max(Map.Entry.comparingByValue())
                                .map(Map.Entry::getKey)
                                .orElse("water");
                    }

                    log.info("Economy Seeding: Creating standing IMPORT request for '{}' to extract resources...", bestGood);
                    TradeRequestCreate req = new TradeRequestCreate(
                            myPlanetId, bestGood, "import", "standing", 10, null, null);

                    return api.createTradeRequest(req).then();
                });
    }

    private Mono<Void> manageInternalLogistics() {
        if (ships.getShipStatuses().size() >= config.getMaxConcurrentShips()) return Mono.empty();

        List<Planet> myPlanets = galaxy.getConnectedPlanets().values().stream()
                .filter(p -> p.station() != null && config.getPlayerId().equals(p.station().ownerId()))
                .collect(Collectors.toList());

        if (myPlanets.size() < 2) return Mono.empty();

        return Flux.fromIterable(myPlanets)
                .flatMap(originPlanet -> {
                    Map<String, Long> inventory = originPlanet.station().inventory();
                    if (inventory == null) return Mono.empty();

                    return Flux.fromIterable(inventory.entrySet())
                            .filter(entry -> entry.getValue() > 100)
                            .flatMap(entry -> {
                                String good = entry.getKey();
                                long reserved = reservedGoods.getOrDefault(good, 0L);
                                long qtyToMove = entry.getValue() - 50 - reserved;

                                if (qtyToMove <= 0) return Mono.empty();

                                Planet destination = myPlanets.stream()
                                        .filter(p -> !p.id().equals(originPlanet.id()))
                                        .filter(p -> p.settlement() != null && p.settlement().economy().demand().containsKey(good))
                                        .findFirst()
                                        .orElse(null);

                                if (destination != null) {
                                    log.info("Logistics: Shipping {} units of {} from {} to {} (Demand Match)",
                                            qtyToMove, good, originPlanet.name(), destination.name());
                                    return ships.hireTrucking(originPlanet.id(), destination.id(), Map.of(good, qtyToMove));
                                }
                                return Mono.empty();
                            });
                })
                .then();
    }

    /**
     * PERIODIC STATUS REPORT
     * Prints the current state of the station's inventory and credits every 3 minutes.
     */
    private Mono<Void> printStationStatusReport() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        return api.getStation(mySystemName, myPlanetId)
                .doOnNext(station -> {
                    log.info("--- STATUS REPORT ---");
                    log.info("Credits: {}", myCredits);
                    log.info("Station: {} on {}", station.name(), myPlanetId);
                    log.info("Inventory:");
                    if (station.inventory() != null && !station.inventory().isEmpty()) {
                        station.inventory().forEach((good, qty) ->
                                log.info("  - {}: {}", good, qty)
                        );
                    } else {
                        log.info("  (Empty)");
                    }
                    log.info("-----------------------");
                })
                .then();
    }

    /**
     * GOAL EVALUATION
     * Checks if we need to upgrade the station or expand to new planets.
     */
    private Mono<Void> evaluateGoals() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        if (currentGoal != null) {
            return executeCurrentGoal();
        }

        return api.getStation(mySystemName, myPlanetId)
                .flatMap(station -> {
                    long totalItems = station.inventory().values().stream().mapToLong(Long::longValue).sum();

                    // Goal 1: Prevent storage overflow (Priority)
                    if (totalItems > station.maxStorage() * 0.8) {
                        log.info("Storage critical (>80%). Setting goal: Upgrade Storage.");
                        Map<String, Long> cost = Map.of("steel", 200L, "electronics", 50L);
                        currentGoal = new Goal("upgrade_storage", myPlanetId, 5000, cost);
                        reservedCredits = currentGoal.requiredCredits();
                        reservedGoods.putAll(currentGoal.requiredGoods());
                        return Mono.empty();
                    }

                    // Goal 2: Expansion to new planets
                    if (myCredits > 20_000) {
                        return Flux.fromIterable(galaxy.getSystems().values())
                                .flatMap(system -> Flux.fromIterable(system.planets()))
                                .filter(p -> "settled".equals(p.status()) && p.station() == null)
                                .next()
                                .doOnNext(targetPlanet -> {
                                    log.info("Wealth accumulated! Setting goal: Expand to {} ({})", targetPlanet.name(), targetPlanet.id());
                                    Map<String, Long> cost = Map.of("steel", 500L, "electronics", 200L);
                                    currentGoal = new Goal("install_station", targetPlanet.id(), 15000, cost);
                                    reservedCredits = currentGoal.requiredCredits();
                                    reservedGoods.putAll(currentGoal.requiredGoods());
                                })
                                .then();
                    }

                    return Mono.empty();
                });
    }

    /**
     * GOAL EXECUTION
     * Checks requirements and triggers the construction API.
     */
    private Mono<Void> executeCurrentGoal() {
        return api.getStation(mySystemName, myPlanetId)
                .flatMap(station -> {
                    if (myCredits < currentGoal.requiredCredits()) {
                        log.debug("Goal '{}' pending: Not enough credits. Have {}, need {}",
                                currentGoal.type(), myCredits, currentGoal.requiredCredits());
                        return Mono.empty();
                    }

                    for (Map.Entry<String, Long> req : currentGoal.requiredGoods().entrySet()) {
                        String good = req.getKey();
                        long requiredQty = req.getValue();
                        long availableQty = station.inventory().getOrDefault(good, 0L);

                        if (availableQty < requiredQty) {
                            long missingQty = requiredQty - availableQty;
                            log.debug("Goal '{}' pending: Missing {}x {} in station.",
                                    currentGoal.type(), missingQty, good);

                            Double marketPrice = market.getPrice(good);
                            long buyPrice = (marketPrice != null) ? Math.round(marketPrice * 1.2) : 50L;

                            log.info("Goal needs materials! Placing buy order for {}x {} @ {}", missingQty, good, buyPrice);
                            return market.placeBuyOrder(good, buyPrice, missingQty, myPlanetId).then();
                        }
                    }

                    log.info("All requirements met! Executing construction: {}", currentGoal.type());

                    Mono<?> actionMono = Mono.empty();

                    if ("upgrade_storage".equals(currentGoal.type())) {
                        actionMono = api.upgradeStation(new UpgradeStationRequest(myPlanetId, "storage"));
                    } else if ("install_station".equals(currentGoal.type())) {
                        Map<String, String> requestBody = Map.of(
                                "source_planet_id", myPlanetId,
                                "target_planet_id", currentGoal.targetPlanetId(),
                                "station_name", "Beta Base Alpha"
                        );
                        actionMono = api.installStation(requestBody);
                    }

                    return actionMono.doOnSuccess(res -> {
                        log.info("Goal '{}' successfully executed!", currentGoal.type());
                        currentGoal = null;
                        reservedCredits = 0;
                        reservedGoods.clear();
                    }).onErrorResume(e -> {
                        log.error("Failed to execute goal '{}': {}", currentGoal.type(), e.getMessage());
                        return Mono.empty();
                    }).then();
                });
    }

    private Mono<Void> manageElevatorTransfers() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        return api.getSpaceElevator(mySystemName, myPlanetId)
                .flatMap(se -> {
                    if (se.warehouse() != null && config.getPlayerId().equals(se.warehouse().ownerId())) {
                        List<TransferItem> items = new ArrayList<>();
                        long capacity = se.config().cabinCapacity();
                        long load = 0;

                        for (Map.Entry<String, Long> entry : se.warehouse().inventory().entrySet()) {
                            long qtyInWarehouse = entry.getValue();
                            if (qtyInWarehouse > 0 && load < capacity) {
                                long qtyToTake = Math.min(qtyInWarehouse, capacity - load);
                                items.add(new TransferItem(entry.getKey(), qtyToTake));
                                load += qtyToTake;
                            }
                        }

                        if (!items.isEmpty()) {
                            log.info("Elevator: Transferring {} units from warehouse to orbit.", load);
                            return elevator.transferToOrbit(mySystemName, myPlanetId, items).then();
                        }
                    }
                    return Mono.empty();
                })
                .onErrorResume(e -> {
                    log.error("Elevator error: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * ACTIVE SELLING (Hybrid: Market Taker & Market Maker)
     * Sells to existing buyers if possible, otherwise places a limit order.
     */
    private Mono<Void> sellStationInventory() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        return api.getStation(mySystemName, myPlanetId)
                .flatMap(station -> {
                    if (station.inventory() == null || station.inventory().isEmpty()) return Mono.empty();

                    return Flux.fromIterable(station.inventory().entrySet())
                            .filter(entry -> entry.getValue() > 0)
                            .flatMap(entry -> {
                                String good = entry.getKey();
                                long totalQty = entry.getValue();

                                long safetyBuffer = 500L;
                                long reserved = reservedGoods.getOrDefault(good, 0L);
                                long sellable = totalQty - reserved - safetyBuffer;

                                if (sellable <= 0) return Mono.empty();

                                return api.getOrderBook(good).flatMap(book -> {
                                    if (book.bids() != null && !book.bids().isEmpty()) {
                                        double bestBidPrice = book.bids().get(0).price();
                                        long maxDemand = book.bids().get(0).totalQuantity();
                                        long qtyToSell = Math.min(sellable, Math.min(maxDemand, 500L));

                                        log.info("INSTANT PROFIT! Found a buyer for {}. Selling {} units @ {}.",
                                                good, qtyToSell, bestBidPrice);
                                        return market.placeSellOrder(good, (long) bestBidPrice, qtyToSell, myPlanetId);
                                    } else {
                                        long qtyToSell = Math.min(sellable, 500L);
                                        Double marketPrice = market.getPrice(good);
                                        long sellPrice = (marketPrice != null) ? Math.round(marketPrice) : 10L;

                                        log.info("Market Maker: No active buyers. Placing Limit Sell Order for {}x {} @ {}.",
                                                qtyToSell, good, sellPrice);
                                        return market.placeSellOrder(good, sellPrice, qtyToSell, myPlanetId);
                                    }
                                });
                            })
                            .then();
                })
                .onErrorResume(e -> Mono.empty());
    }

    private Mono<Void> refreshPlayerProfile() {
        return api.getPlayer().doOnNext(p -> this.myCredits = p.credits()).then();
    }
}