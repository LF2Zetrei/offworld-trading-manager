package com.offworld.strategy;

import com.offworld.api.ApiClient;
import com.offworld.config.AppConfig;
import com.offworld.model.*;
import com.offworld.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
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

    // ─── LOCKS AND STATES ──────────────────────────────────────────────────────
    private final Set<String> blockedSales = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> reservedForBuild = new ConcurrentHashMap<>();

    // ─── REACTIVE QUEUES ────────────────────────────────────
    private final Sinks.Many<ConstructionTask> constructionQueue = Sinks.many().multicast().onBackpressureBuffer();
    private final Sinks.Many<DeliveryTask> deliveryQueue = Sinks.many().multicast().onBackpressureBuffer();
    private final Sinks.Many<PurchaseTask> purchaseQueue = Sinks.many().multicast().onBackpressureBuffer();
    private final Sinks.Many<PlanetSearchTask> planetSearchQueue = Sinks.many().multicast().onBackpressureBuffer();

    public record ConstructionTask(String targetPlanetId, String projectType, Map<String, Long> requiredResources) {}
    public record DeliveryTask(String goodName, long quantity, String targetPlanetId, double sellPrice) {}
    public record PurchaseTask(String goodName, long quantityToBuy, double maxPricePerUnit) {}
    public record PlanetSearchTask(String highlyDemandedGood) {}

    // Track recent prices per good from SSE
    private final Map<String, Deque<Double>> priceHistory = new ConcurrentHashMap<>();
    // Track our own open orders: orderId -> order
    private final Map<String, MarketOrder> openOrders = new ConcurrentHashMap<>();
    // Active trucking ship count
    private final AtomicInteger activeShips = new AtomicInteger(0);


    // ─── GOAL SYSTEM (Added back for proper progression) ───────────────────────
    private Goal currentGoal = null;
    private final Map<String, Long> reservedGoods = new ConcurrentHashMap<>();
    private long reservedCredits = 0;

    private record Goal(String type, String targetPlanetId, long requiredCredits, Map<String, Long> requiredGoods) {}

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
        // ─── QUEUES CONSUMERS ────────────────────────────────

        deliveryQueue.asFlux()
                .flatMap(this::processDelivery)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in delivery consumer", e));

        constructionQueue.asFlux()
                .flatMap(this::processConstruction)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in construction consumer", e));

        // 1. React to SSE trade events
        market.tradeEvents()
                .doOnNext(this::onTradeEvent)
                .subscribeOn(Schedulers.parallel())
                .subscribe(null, e -> log.error("Error in trade events stream", e));

        // 2. SCANNERS (Intervals)
        Flux.interval(Duration.ofMillis(config.getMarketRefreshIntervalMs()))
                .flatMap(tick -> scanAndTrade().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in scanAndTrade loop", e));

        Flux.interval(Duration.ofMillis(config.getMarketRefreshIntervalMs() + 500))
                .flatMap(tick -> scanMarketForDeliveries().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in scanMarketForDeliveries loop", e));

        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> evaluateGoals().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in evaluateGoals loop", e));

        // 3. Monitor our orders
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> refreshOpenOrders().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in refreshOpenOrders loop", e));

        // 4. Seed trade requests to generate supply
        Flux.interval(Duration.ofMillis(config.getTradeRequestIntervalMs()))
                .flatMap(tick -> manageTradeRequests().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in manageTradeRequests loop", e));

        // 5. Leaderboard check
        Flux.interval(Duration.ofMinutes(1))
                .flatMap(tick -> api.getLeaderboard())
                .doOnNext(board -> board.stream()
                        .filter(e -> e.playerId().equals(config.getPlayerId()))
                        .findFirst()
                        .ifPresent(e -> log.info("Leaderboard: rank={}, profit={}",
                                board.indexOf(e) + 1, e.profit())))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in leaderboard check", e));

        // 6. Update credits every 15 seconds
        Flux.interval(Duration.ofSeconds(15))
                .flatMap(tick -> refreshPlayerProfile().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in refreshPlayerProfile", e));

        // 7. Check the surface and use the elevator every 20 seconds
        Flux.interval(Duration.ofSeconds(20))
                .flatMap(tick -> manageElevatorTransfers().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in manageElevatorTransfers", e));

        // 8. Status Report every 2 minutes
        Flux.interval(Duration.ofMinutes(2))
                .flatMap(tick -> printStationStatusReport().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in status report loop", e));

        // Sell station's inventory every 30 seconds
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> sellStationInventory().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in sellStationInventory", e));

        Flux.interval(Duration.ofSeconds(45))
                .flatMap(tick -> dispatchShipToEarth().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in dispatchShipToEarth", e));

        Flux.interval(Duration.ofMinutes(5))
                .flatMap(tick -> galaxy.exploreGalaxy().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in galaxy refresh loop", e));

        log.info("All trading pipelines and queue consumers launched");
    }

    private void onTradeEvent(TradeEvent event) {
        priceHistory.computeIfAbsent(event.goodName(), k -> new ArrayDeque<>(20)).offer(event.price());
        Deque<Double> hist = priceHistory.get(event.goodName());
        while (hist.size() > 20) hist.poll();
    }

    // ─── CONSUMERS LOGIC ──────────────────────────────────────────

    private Mono<Void> processDelivery(DeliveryTask task) {
        if (blockedSales.contains(task.goodName())) {
            log.debug("Delivery ignored: {} is blocked for construction.", task.goodName());
            return Mono.empty();
        }

        if (ships.getShipStatuses().size() >= config.getMaxConcurrentShips()) {
            log.debug("Delivery ignored: No ships available.");
            return Mono.empty();
        }

        log.info("Processing delivery: Selling {}x {} to {}", task.quantity(), task.goodName(), task.targetPlanetId());
        long sellPrice = (long) Math.floor(task.sellPrice());

        return market.placeSellOrder(task.goodName(), sellPrice, task.quantity(), myPlanetId)
                .delayElement(Duration.ofSeconds(2))
                .flatMap(order -> ships.hireTrucking(myPlanetId, task.targetPlanetId(), Map.of(task.goodName(), task.quantity())))
                .then()
                .onErrorResume(e -> Mono.empty());
    }

    private Mono<Void> processConstruction(ConstructionTask task) {
        log.info("Processing construction project: {} on {}", task.projectType(), task.targetPlanetId());

        task.requiredResources().forEach((good, qty) -> {
            blockedSales.add(good);
            reservedForBuild.put(good, qty);
        });

        UpgradeStationRequest upgradeReq = new UpgradeStationRequest(task.targetPlanetId(), task.projectType());
        return api.upgradeStation(upgradeReq)
                .doOnSuccess(res -> {
                    log.info("Upgrade {} successful!", task.projectType());
                    task.requiredResources().keySet().forEach(blockedSales::remove);
                    task.requiredResources().keySet().forEach(reservedForBuild::remove);
                })
                .doOnError(e -> {
                    log.warn("Upgrade {} failed, keeping locks. Need more resources?", task.projectType());
                })
                .then()
                .onErrorResume(e -> Mono.empty());
    }

    // ─── SCANNERS ─────────────────────────────────────────────────────────────

    private Mono<Void> scanMarketForDeliveries() {
        return api.getMarketPrices()
                .flatMap(prices -> Flux.fromIterable(prices.keySet())
                        .flatMap(good -> api.getOrderBook(good).onErrorResume(e -> Mono.empty()), 5)
                        .filter(book -> book.bids() != null && !book.bids().isEmpty())
                        .flatMap(book -> {
                            OrderBookLevel bestBid = book.bids().get(0);
                            if (bestBid.price() > 0) {
                                return Flux.fromIterable(galaxy.getConnectedPlanets().values())
                                        .filter(p -> !p.id().equals(myPlanetId))
                                        .next()
                                        .doOnNext(targetPlanet -> {
                                            DeliveryTask task = new DeliveryTask(book.goodName(), 10, targetPlanet.id(), bestBid.price());
                                            deliveryQueue.tryEmitNext(task);
                                            log.info("Export from {} to {}", book.goodName(), targetPlanet.id());
                                        })
                                        .switchIfEmpty(Mono.defer(() -> {
                                            log.warn("Can't deliver {} : no planet to deliver found", book.goodName());
                                            return Mono.empty();
                                        }))
                                        .then();
                            }
                            return Mono.empty();
                        })
                        .then());
    }

    private Mono<Void> scanStationForUpgrades() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        return api.getStation(mySystemName, myPlanetId)
                .doOnNext(station -> {
                    long totalItems = station.inventory().values().stream().mapToLong(Long::longValue).sum();
                    long maxStorage = station.maxStorage();

                    if (totalItems >= maxStorage * 0.9) {
                        Map<String, Long> requiredForStorage = Map.of("iron_ore", 100L);
                        constructionQueue.tryEmitNext(new ConstructionTask(myPlanetId, "storage_upgrade", requiredForStorage));
                    } else if (totalItems == 0) {
                        Map<String, Long> requiredForElevator = Map.of("steel", 200L);
                        constructionQueue.tryEmitNext(new ConstructionTask(myPlanetId, "space_elevator", requiredForElevator));
                    }
                })
                .then();
    }

    // ─── BASE FUNCTIONS ───────────────────────────────────────

    private Mono<Void> scanAndTrade() {
        return api.getMarketPrices()
                .flatMap(prices -> {
                    if (prices.isEmpty()) return Mono.empty();

                    List<String> goods = new ArrayList<>(prices.keySet());
                    return Flux.fromIterable(goods)
                            .flatMap(good -> api.getOrderBook(good)
                                        .map(book -> evaluateSpread(good, book))
                                    .onErrorResume(e -> Mono.empty()), 5)
                            .filter(opp -> opp != null)
                            .sort(Comparator.comparingDouble(Opportunity::estimatedProfit).reversed())
                            .next()
                            .flatMap(this::executeOpportunity);
                })
                .then();
    }

    private record Opportunity(String goodName, double buyPrice, double sellPrice,
                               long quantity, double estimatedProfit) {}

    private Opportunity evaluateSpread(String goodName, OrderBook book) {
        if (book.asks() == null || book.asks().isEmpty()) return null;
        if (book.bids() == null || book.bids().isEmpty()) return null;

        double bestAsk = book.asks().get(0).price();
        double bestBid = book.bids().get(0).price();

        double spread = bestBid - bestAsk;
        double marginPct = bestAsk > 0 ? spread / bestAsk : 0;

        if (marginPct < config.getMinProfitMargin()) return null;

        long availableQty = Math.min(book.asks().get(0).totalQuantity(),
                book.bids().get(0).totalQuantity());
        long maxAffordable = (long)(myCredits * config.getMaxOrderCreditsPct() / bestAsk);
        long qty = Math.min(availableQty, Math.max(1, maxAffordable));

        double estimatedProfit = qty * spread * 0.9;
        return (qty > 0) ? new Opportunity(goodName, bestAsk, bestBid, qty, estimatedProfit) : null;
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
        if (mySystemName == null || myPlanetId == null) return Mono.empty();
        return api.getTradeRequests()
                .flatMap(requests -> {
                    boolean hasActive = requests.stream().anyMatch(r -> "active".equals(r.status()));
                    if (hasActive)
                        return Mono.empty();
                    return api.getStation(mySystemName, myPlanetId).flatMap(station -> {
                        if(station.inventory() == null || station.inventory().isEmpty())
                            return Mono.empty();

                        String firstAvailableResource = station.inventory().keySet().iterator().next();
                        TradeRequestCreate req = new TradeRequestCreate(myPlanetId, firstAvailableResource, "import", "standing", 5, null, null);
                        return api.createTradeRequest(req)
                                .doOnSuccess(r -> log.info("Trade request created: {} (standing mode)", firstAvailableResource))
                                .then();
                    });
        })
        .onErrorResume(e -> Mono.empty());
    }

    private Mono<Void> executeOpportunity(Opportunity opp) {
        if (ships.getShipStatuses().size() >= config.getMaxConcurrentShips()) {
            log.debug("Ship limit reached. Skipping trade opportunity.");
            return Mono.empty();
        }

        return Flux.fromIterable(galaxy.getConnectedPlanets().values())
                .filter(p -> !p.id().equals(myPlanetId))
                .next()
                .switchIfEmpty(Mono.defer(() -> {
                    log.warn("Opportunité annulée pour {} : aucune planète valide trouvée.", opp.goodName());
                    return Mono.empty();
                }))
                .flatMap(targetPlanet -> {
                    long buyPrice = (long) Math.ceil(opp.buyPrice());
                    log.info("Interplanetary Trade: Buying {}x {} @ {} on {}",
                            opp.quantity(), opp.goodName(), buyPrice, targetPlanet.name());

                    return market.placeBuyOrder(opp.goodName(), buyPrice, opp.quantity(), targetPlanet.id())
                            .onErrorResume(e -> {
                                log.warn("Skipping trade for {}: {}", opp.goodName(), e.getMessage());
                                return Mono.empty();
                            })
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

    private Mono<Void> dispatchShipToEarth() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();
        if (!ships.getShipStatuses().isEmpty()) return Mono.empty();

        return api.getStation(mySystemName, myPlanetId)
                .flatMap(station -> {
                    if (station.inventory() == null || station.inventory().isEmpty()) return Mono.empty();

                    Map.Entry<String, Long> resourceToSend = station.inventory().entrySet().stream()
                            .filter(e -> e.getValue() > 0)
                            .findFirst()
                            .orElse(null);

                    if (resourceToSend != null && resourceToSend.getValue() >= 500) {
                        return Flux.fromIterable(galaxy.getConnectedPlanets().values())
                                .filter(p -> !p.id().equals(myPlanetId))
                                .next()
                                .switchIfEmpty(Mono.defer(() -> {
                                    log.warn("No planet found");
                                    return Mono.empty();
                                }))
                                .flatMap(targetPlanet ->
                                        ships.hireTrucking(myPlanetId, targetPlanet.id(), Map.of(resourceToSend.getKey(), 500L)).then()
                                );
                    }
                    return Mono.empty();
                })
                .onErrorResume(e -> Mono.empty());
    }

    private Mono<Void> refreshPlayerProfile() {
        return api.getPlayer()
                .doOnNext(player -> this.myCredits = player.credits())
                .then();
    }

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
                            return elevator.transferToOrbit(mySystemName, myPlanetId, itemsToTransfer).then();
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
                            .filter(entry -> !blockedSales.contains(entry.getKey()))
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
    // ─── GOAL MANAGEMENT ─────────────────────────────────────────────

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
                        reservedGoods.clear(); // Reset before adding
                        reservedGoods.putAll(currentGoal.requiredGoods());
                        return Mono.empty();
                    }

                    // Goal 2: Expansion to new planets (Dynamic Search)
                    if (myCredits > 50_000) {
                        return Flux.fromIterable(galaxy.getSystems().values())
                                .flatMap(system -> Flux.fromIterable(system.planets()))
                                // We look for an empty planet to colonize
                                .filter(p -> "uninhabited".equals(p.status()) && p.station() == null)
                                .next()
                                .doOnNext(targetPlanet -> {
                                    log.info("Wealth accumulated! Setting goal: Expand to {} ({})", targetPlanet.name(), targetPlanet.id());
                                    // Base cost for a settlement
                                    Map<String, Long> cost = Map.of("steel", 1000L, "electronics", 500L, "food", 200L, "water", 200L);
                                    currentGoal = new Goal("found_settlement", targetPlanet.id(), 30000, cost);
                                    reservedCredits = currentGoal.requiredCredits();
                                    reservedGoods.clear();
                                    reservedGoods.putAll(currentGoal.requiredGoods());
                                })
                                .then();
                    }

                    return Mono.empty();
                });
    }

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

                        long alreadyOrderedQty = openOrders.values().stream()
                                .filter(o -> o.goodName().equals(good) && "buy".equals(o.side()))
                                .mapToLong(o -> o.quantity() - o.filledQuantity())
                                .sum();

                        long totalIncoming = availableQty + alreadyOrderedQty;

                        if (totalIncoming < requiredQty) {
                            long missingQty = requiredQty - totalIncoming;
                            log.debug("Goal '{}' pending: Missing {}x {} in station.", currentGoal.type(), missingQty, good);

                            Double marketPrice = market.getPrice(good);
                            long buyPrice = (marketPrice != null) ? Math.round(marketPrice * 1.2) : 50L;

                            log.info("Goal needs materials! Placing buy order for {}x {} @ {}", missingQty, good, buyPrice);
                            return market.placeBuyOrder(good, buyPrice, missingQty, myPlanetId)
                                    .onErrorResume(e -> {
                                        log.warn("Cannot buy {} right now. Reason: {}", good, e.getMessage());
                                        return Mono.empty();
                                    })
                                    .doOnNext(o -> { if(o != null) openOrders.put(o.id(), o); })
                                    .then();
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
                    } else if ("found_settlement".equals(currentGoal.type())) {
                        Map<String, Object> requestBody = Map.of(
                                "source_planet_id", myPlanetId,
                                "target_planet_id", currentGoal.targetPlanetId(),
                                "settlement_name", "New Beta Colony",
                                "station_name", "Beta Gateway",
                                "extra_goods", Map.of("food", 200, "water", 200)
                        );
                        actionMono = api.foundSettlement(requestBody);
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

    /**
     * PERIODIC STATUS REPORT
     * Prints the current state of the station's inventory and credits every 3 minutes.
     */
    private Mono<Void> printStationStatusReport() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        return api.getStation(mySystemName, myPlanetId)
                .doOnNext(station -> {
                    log.info("================ STATUS REPORT ================");
                    log.info("Credits: {}", myCredits);
                    log.info("Station: {} (Planet: {})", station.name(), myPlanetId);
                    log.info("Inventory:");
                    if (station.inventory() != null && !station.inventory().isEmpty()) {
                        station.inventory().forEach((good, qty) -> {
                            long reserved = reservedGoods.getOrDefault(good, 0L);
                            if (reserved > 0) {
                                log.info("  - {}: {} ({} reserved for goals)", good, qty, reserved);
                            } else {
                                log.info("  - {}: {}", good, qty);
                            }
                        });
                    } else {
                        log.info("  (Empty)");
                    }
                    if (currentGoal != null) {
                        log.info("Current Goal: {}", currentGoal.type());
                    }
                    log.info("===============================================");
                })
                .onErrorResume(e -> {
                    log.error("Failed to fetch status report: {}", e.getMessage());
                    return Mono.empty();
                })
                .then();
    }
}