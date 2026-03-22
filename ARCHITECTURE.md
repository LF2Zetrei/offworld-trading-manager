# Architecture Document — Offworld Trading Manager

## Overview

The application is a reactive pipeline orchestrator built on **Project Reactor**.
All I/O is non-blocking. Data flows through the system as Mono/Flux streams, with
explicit scheduler assignment for blocking operations.

---

## Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TradingManager (main)                        │
│  Wires all components together and holds the JVM alive              │
└────────────────────────┬────────────────────────────────────────────┘
                         │
        ┌────────────────┼──────────────────────┐
        │                │                      │
        ▼                ▼                      ▼
 ┌─────────────┐  ┌─────────────┐      ┌──────────────────┐
 │  ApiClient  │  │WebhookServer│      │  AppConfig       │
 │ (Reactor    │  │ (Vert.x)    │      │ (properties/env) │
 │  Netty)     │  │             │      └──────────────────┘
 └──────┬──────┘  └──────┬──────┘
        │                │ Sinks.Many<Event>
        │                │
  ┌─────▼────────────────▼──────────────────────────────────┐
  │                      Services                           │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
  │  │GalaxyService │  │MarketService │  │ ShipService  │  │
  │  │              │  │              │  │              │  │
  │  │ explore()    │  │ tradeEvents()│  │ lifecycle    │  │
  │  │ Flux<Planet> │  │ Flux<Trade>  │  │ management   │  │
  │  └──────────────┘  └──────────────┘  └──────────────┘  │
  │  ┌──────────────┐                                       │
  │  │ElevatorSvc   │                                       │
  │  │ transfer()   │                                       │
  │  │ boundedElastic│                                      │
  │  └──────────────┘                                       │
  └─────────────────────────┬───────────────────────────────┘
                            │
                   ┌────────▼────────┐
                   │ TradingStrategy │
                   │  (automated)    │
                   └─────────────────┘
```

---

## Reactive Patterns Used

### 1. Parallel Fan-Out (Galaxy Exploration)

```
api.getSystems()                 // Mono<List<StarSystem>>
  .flatMapMany(Flux::fromIterable)
  .flatMap(sys -> api.getSystem(sys.name()))  // parallel, default concurrency
  .flatMapMany(sys -> Flux.fromIterable(sys.planets()))
  .filter(p -> "connected".equals(p.status().status()))
```

All system details are fetched in parallel using `flatMap`'s default concurrency.
Independent I/O operations never wait for each other.

### 2. SSE Backpressure Handling

```
httpClient.get().uri("/market/trades")
  .responseContent().asString()
  .filter(line -> line.startsWith("data:"))
  .onBackpressureDrop(dropped -> log.debug("Dropped: {}", dropped.id()))
```

The trade stream can emit faster than the strategy can process. `onBackpressureDrop`
discards excess events rather than blocking the Netty I/O thread. Price history is
updated on every event; dropped events only affect trend granularity, not correctness.

### 3. Webhook → Reactive Bridge (Sinks)

```
// Vert.x handler (imperative) pushes into a hot sink:
originDockingSink.tryEmitNext(event);

// Strategy subscribes reactively:
webhookServer.originDockingRequests()
  .flatMap(e -> authorizeDock(e.shipId()))
  .subscribe();
```

`Sinks.Many.multicast()` bridges the imperative Vert.x world into reactive Flux.
Multiple independent consumers can subscribe without coordination.

### 4. Blocking I/O Offloading (Space Elevator)

```
api.transferElevator(system, planet, request)
  .subscribeOn(Schedulers.boundedElastic())  // offload to thread pool
  .flatMap(result -> result.success()
      ? Mono.just(result)
      : Mono.error(new ElevatorFailureException(...)))
  .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(5))
      .filter(e -> e instanceof ElevatorFailureException))
```

The HTTP call blocks for `transfer_duration_secs`. `subscribeOn(boundedElastic())`
ensures this blocking happens on a dedicated thread pool, never the Netty event loop.
The elevator client uses a 120-second `responseTimeout`.

### 5. Polling with Retry

```
Flux.interval(Duration.ofMillis(config.getShipPollingIntervalMs()))
  .flatMap(tick -> api.getShips())
  .flatMapMany(Flux::fromIterable)
  .filter(ship -> isActionable(ship.status()))
  .flatMap(ship -> advanceShip(ship))
  .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(10)))
```

`Flux.interval` creates a scheduled emission. Ships stuck in `awaiting_*_auth` states
are advanced automatically. `retryWhen` with `Long.MAX_VALUE` ensures the polling loop
never permanently stops.

### 6. Error Handling Strategy

| Scenario | Handling |
|---|---|
| HTTP 503 (no docking bay) | Retry with fixed delay, filter by ApiException.isUnavailable() |
| HTTP 4xx (bad request) | Propagate error, log and skip |
| SSE stream disconnect | Retry with exponential backoff, max 30s |
| Elevator cabin failure | Retry up to N times (configurable) |
| Galaxy exploration failure | Retry 3 times with 5s delay |
| Polling loop failure | Infinite retry with 10s delay |

### 7. Concurrency Model

```
Thread Pool          Usage
─────────────────────────────────────────────────────
Netty event loop     All non-blocking HTTP I/O
boundedElastic       Space elevator blocking calls
parallel             SSE event processing (CPU-bound)
single (Vert.x)      Webhook server I/O
```

Netty's event loop is never blocked. Blocking operations are explicitly offloaded
via `subscribeOn(Schedulers.boundedElastic())`.

---

### 8. Goal-Based Resource Reservation
```
To prevent the bot from selling resources required for construction, a reservation system was added to the pipeline:
- Evaluation: evaluateGoals() periodically scans for storage issues or expansion opportunities.
- Locking: Resources are flagged in reservedGoods. All selling methods (sellStationInventory) subtract these reservations from the available total.
- Acquisition: If a goal is active but goods are missing, the bot reactively places BUY orders to pull materials from the market.
```
## Data Flow: Market Trade → Order Placement

```
SSE stream                 MarketService              TradingStrategy
    │                           │                           │
    │  "data:{...}"             │                           │
    ├──► parse TradeEvent       │                           │
    │         │                 │                           │
    │         ├──► update lastPrices                        │
    │         │                 │                           │
    │         └──► tradeSink.tryEmitNext()                  │
    │                           │                           │
    │                    tradeEvents() Flux                 │
    │                           └───────────────────────────►
    │                                                       │
    │                                                 onTradeEvent()
    │                                                 → update priceHistory
    │
  [periodic]
    │
    └── scanAndTrade()
          │
          ├── getMarketPrices()
          │     └── flatMap over goods
          │           └── getOrderBook() [parallel, concurrency=5]
          │                 └── evaluateSpread()
          │                       └── if profitable:
          │                             ├── placeBuyOrder()
          │                             └── placeSellOrder()
```

---

## Data Flow: Ship Lifecycle

```
Server                 WebhookServer          ShipService
  │                         │                     │
  │  POST /webhooks          │                     │
  │  OriginDockingRequest    │                     │
  ├────────────────────────► │                     │
  │                    originDockingSink            │
  │                          └─────────────────────►
  │                                           authorizeDock()
  │                                                 │
  │  PUT /ships/{id}/dock ◄─────────────────────────┘
  │                                                 
  │  ShipDocked webhook                             
  ├────────────────────────► │                     │
  │                    shipDockedSink               │
  │                          └─────────────────────►
  │                                          scheduleUndock()
  │                                          [polls until timer expires]
  │                                                 │
  │  PUT /ships/{id}/undock ◄───────────────────────┘
```

The webhook-driven path is the primary path. The polling safety net catches any missed
webhooks by scanning all ships every 2 seconds.

---

## Key Design Decisions

1. **Reactor over RxJava/Mutiny**: Reactor is the de-facto standard in the Java
   ecosystem, with the best Netty integration and scheduler control.

2. **Vert.x for webhook server**: Vert.x is a lightweight, non-blocking HTTP server
   that integrates naturally with Reactor via shared event-loop principles. It avoids
   the weight of Spring Boot while providing production-quality HTTP serving.

3. **Hot Sinks for webhook events**: Webhook callbacks arrive on Vert.x threads.
   `Sinks.Many.multicast()` cleanly bridges this into reactive Flux without
   requiring shared mutable state or synchronized queues.

4. **Two-client HTTP setup**: A standard client (60s timeout) handles all REST calls.
   A dedicated elevator client (120s timeout) handles the blocking transfer endpoint.
   This prevents aggressive timeouts from killing legitimate long-running transfers.

5. **Polling + Webhooks**: Webhooks are primary; polling is a safety net. This covers
   network hiccups where a webhook might not arrive, ensuring ships never deadlock.
