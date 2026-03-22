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
import com.offworld.model.Models.*;

/**
 * Automated trading strategy - Full Reactive Architecture.
 * Synchronized with Rust Server documentation (iron_ore + total mode).
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

        Flux.interval(Duration.ofSeconds(45))
                .flatMap(tick -> scanStationForUpgrades().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in scanStationForUpgrades loop", e));

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

        // Sell station's inventory every 30 seconds
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> sellStationInventory().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in sellStationInventory", e));

        Flux.interval(Duration.ofSeconds(45))
                .flatMap(tick -> dispatchShipToEarth().onErrorResume(e -> Mono.empty()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.error("Error in dispatchShipToEarth", e));

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
                        .doOnNext(book -> {
                            OrderBookLevel bestBid = book.bids().get(0);
                            if (bestBid.price() > 0) {
                                // On utilise Sol-4 (Mars) comme cible valide
                                DeliveryTask task = new DeliveryTask(book.goodName(), 10, "Sol-4", bestBid.price());
                                deliveryQueue.tryEmitNext(task);
                            }
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

    private Mono<Void> executeOpportunity(Opportunity opp) {
        if (ships.getShipStatuses().size() >= config.getMaxConcurrentShips()) return Mono.empty();

        return Flux.fromIterable(galaxy.getConnectedPlanets().values())
                .filter(p -> !p.id().equals(myPlanetId))
                .next()
                .flatMap(targetPlanet -> {
                    long buyPrice = (long) Math.ceil(opp.buyPrice());

                    return market.placeBuyOrder(opp.goodName(), buyPrice, opp.quantity(), targetPlanet.id())
                            .doOnNext(o -> openOrders.put(o.id(), o))
                            .delayElement(Duration.ofSeconds(5))
                            .flatMap(order -> ships.hireTrucking(targetPlanet.id(), myPlanetId, Map.of(opp.goodName(), opp.quantity())));
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

    private Mono<Void> manageTradeRequests() {
        return api.getTradeRequests()
                .flatMap(requests -> {
                    boolean hasActive = requests.stream().anyMatch(r -> "active".equals(r.status()));
                    if (!hasActive) {
                        // FIX DEFINITIF: iron_ore (nom correct) + total (mode supporté par le serveur)
                        TradeRequestCreate req = new TradeRequestCreate(
                                myPlanetId, "iron_ore", "export",
                                "total", 5, null, null);
                        return api.createTradeRequest(req)
                                .doOnSuccess(r -> log.info("Trade request created: iron_ore (total mode)"))
                                .then();
                    }
                    return Mono.empty();
                });
    }

    private Mono<Void> dispatchShipToEarth() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();
        if (!ships.getShipStatuses().isEmpty()) return Mono.empty();

        return api.getStation(mySystemName, myPlanetId)
                .flatMap(station -> {
                    long ironQty = station.inventory().getOrDefault("iron_ore", 0L);
                    if (ironQty >= 500) {
                        return ships.hireTrucking(myPlanetId, "Sol-3", Map.of("iron_ore", 500L)).then();
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
                .onErrorResume(e -> Mono.empty());
    }

    private Mono<Void> sellStationInventory() {
        if (mySystemName == null || myPlanetId == null) return Mono.empty();

        return api.getStation(mySystemName, myPlanetId)
                .flatMap(station -> {
                    if (station.inventory() == null || station.inventory().isEmpty()) return Mono.empty();

                    return Flux.fromIterable(station.inventory().entrySet())
                            .filter(entry -> entry.getValue() > 0)
                            .filter(entry -> !entry.getKey().equals("iron_ore")) // Protection stock de base
                            .filter(entry -> !blockedSales.contains(entry.getKey()))
                            .flatMap(entry -> {
                                String good = entry.getKey();
                                long qty = entry.getValue();
                                Double currentPrice = market.getPrice(good);
                                long sellPrice = (currentPrice != null) ? Math.round(currentPrice) : 10L;

                                return market.placeSellOrder(good, sellPrice, qty, myPlanetId);
                            })
                            .then();
                })
                .onErrorResume(e -> Mono.empty());
    }
}