# Offworld Trading Manager — Reactive Java Client

A fully reactive Java application that plays the Offworld Trading Manager game,
implementing all five server interaction patterns: synchronous REST, blocking HTTP
(space elevator), async polling, SSE streaming, and webhooks.

---

## Reactive Library: Project Reactor

**Why Reactor?**

- Industry standard for reactive JVM apps (Spring WebFlux, R2DBC)
- Excellent operator set: `flatMap`, `zip`, `merge`, `retry`, `timeout`, `buffer`
- Native integration with Reactor Netty for non-blocking HTTP
- Sinks API for bridging imperative webhook callbacks into reactive pipelines
- First-class `Schedulers` control for offloading blocking I/O

---

## Project Structure

```
src/main/java/com/offworld/
├── TradingManager.java          # Entry point & startup sequence
├── api/
│   ├── ApiClient.java           # All REST + SSE calls via Reactor Netty
│   └── ApiException.java        # HTTP error wrapper
├── config/
│   └── AppConfig.java           # Properties + env-var config
├── model/
│   ├── Models.java              # All API request/response records
│   └── WebhookEvents.java       # Webhook payload records
├── service/
│   ├── GalaxyService.java       # Galaxy exploration & caching
│   ├── MarketService.java       # SSE trade stream + order management
│   ├── ShipService.java         # Ship lifecycle (webhooks + polling)
│   └── ElevatorService.java     # Blocking elevator transfers
├── strategy/
│   └── TradingStrategy.java     # Automated trading loop
└── webhook/
    └── WebhookServer.java       # Vert.x HTTP server for callbacks
```

---

## Prerequisites

- Java 17+
- Maven 3.8+

---

## Build

```bash
mvn clean package
```

This produces `target/offworld-trading-manager-1.0.0-jar-with-dependencies.jar`.

---

## Configure

Copy the template and fill in your credentials:

```bash
cp src/main/resources/application.properties.template \
   src/main/resources/application.properties
```

Edit `application.properties`:

```properties
server.base-url=http://<server-host>:<port>
player.id=your-player-id
player.api-key=your-api-key
webhook.public-url=http://<your-machine>:9090/webhooks
```

Alternatively, set environment variables (uppercase, dots/dashes become underscores):

```bash
export SERVER_BASE_URL=http://localhost:8080
export PLAYER_ID=alpha-team
export PLAYER_API_KEY=d4f8e2a1-...
export WEBHOOK_PUBLIC_URL=http://192.168.1.10:9090/webhooks
```

---

## Run

```bash
java -jar target/offworld-trading-manager-1.0.0-jar-with-dependencies.jar
```

---

## 🚀 Live Demonstration Scenario (For Evaluation)

The bot is designed to run autonomously. To observe its real-time event-driven capabilities without waiting for global market shifts, execute the following commands in a separate terminal.

- **Triggering Revenue (External Buyer Simulation)** — The bot actively produces and places sell orders for its goods on Earth (`Sol-3`). This command simulates a competitor (`alpha-team`) buying those goods, instantly increasing our bot's profit.
```bash
  curl -X POST http://localhost:8080/market/orders \
    -H "Authorization: Bearer alpha-secret-key-001" \
    -H "Content-Type: application/json" \
    -d '{
      "player_id": "alpha-team",
      "api_key": "alpha-secret-key-001",
      "good_name": "iron_ore",
      "side": "buy",
      "order_type": "market",
      "quantity": 500,
      "station_planet_id": "Sol-3"
    }'
```

**Observation**: The bot's periodic STATUS REPORT will reflect a higher credit balance in the next tick.

**Testing Webhook Reactivity** (0ms Latency) — To prove the Event-Driven architecture bypasses standard polling intervals, we inject a direct webhook payload representing an arriving ship.

```bash
curl -X POST http://localhost:9090/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "type": "origin_docking_request",
    "ship_id": "123e4567-e89b-12d3-a456-426614174000",
    "origin_planet_id": "Proxima Centauri-1",
    "destination_planet_id": "Sol-3",
    "cargo": {"water": 10}
  }'
```
**Observation**: The bot's Vert.x server will parse the JSON and instantly trigger the ShipService to authorize the dock via the REST API, demonstrating end-to-end reactive bridging.


## Configuration Reference

| Property | Default | Description |
|---|---|---|
| `server.base-url` | `http://localhost:8080` | Game server URL |
| `player.id` | *(required)* | Your player ID |
| `player.api-key` | *(required)* | Your API key |
| `webhook.host` | `0.0.0.0` | Webhook server bind address |
| `webhook.port` | `9090` | Webhook server port |
| `webhook.public-url` | `http://localhost:9090/webhooks` | URL registered with game server |
| `polling.ship-status-interval-ms` | `2000` | Ship polling frequency |
| `polling.construction-status-interval-ms` | `5000` | Construction polling |
| `polling.market-refresh-interval-ms` | `10000` | Market scan frequency |
| `strategy.min-profit-margin` | `0.10` | Minimum 10% spread to trade |
| `strategy.max-order-credits-pct` | `0.20` | Max 20% of credits per order |
| `http.elevator-timeout-ms` | `120000` | Elevator long-poll timeout |

---

## How It Works

### Startup Sequence

1. Load config → create `ApiClient` (Reactor Netty) + `WebhookServer` (Vert.x)
2. Start webhook HTTP server on configured port
3. Connect SSE trade stream (`GET /market/trades`) with auto-reconnect
4. Start ship webhook listeners + polling safety net
5. Explore galaxy in parallel (`GET /systems` → fan-out to all systems)
6. Find owned station, register `callback_url` with server
7. Launch all periodic pipelines

### Trading Strategy

<<<<<<< HEAD
The strategy runs three parallel loops:
- **SSE reaction** — updates price history on every trade event
- **Market scan** — every 10s, checks order books for profitable spreads and places limit buy+sell pairs
- **Trade requests** — seeds standing export requests to maintain supply
The application implements a sophisticated autonomous loop:
- **SSE Market Reaction** — Updates local price history to identify high-value goods.
- **Economy Seeding** —  Uses IMPORT trade requests to stimulate planetary production and fill the warehouse.
- **Hybrid Selling Strategy** —
    - **Market Taker**: Sells instantly to existing buyers (Bids) for immediate profit.
    - **Market Maker**: Places Limit Sell orders for excess stock while maintaining a 500-unit safety buffer.
- **Infrastructure Expansion** —  Automatically detects "Settled" planets and triggers station construction once credit and resource thresholds are met.
=======
The strategy is currently implemented as a deterministic "Seed & Trade" loop, focusing on specific resource cycles and hardcoded logistics to validate the five interaction patterns.
Operational Loops

 - SSE Price Monitoring — Continuously listens to `GET /market/trades` to populate a local `priceHistory` buffer (last 20 points). This builds the data foundation for future spread analysis.

 - Fixed-Target Market Scanning — Every 10s, the client polls order books for all known commodities. It attempts to find a 10% profit margin to place limit buy/sell order pairs. In the absence of market data, it defaults to a baseline sell price (10 credits).

 - Hardcoded Supply Seeding — The strategy attempts to maintain a continuous supply of `iron_ore` via the `/trade` endpoint. It uses the `total` mode in `export` direction to generate local inventory, ensuring compatibility with the current server's strict validation.

 - Automated Earth Dispatch — A dedicated monitor checks the station's inventory for `iron_ore`. Once 500 units are accumulated, it triggers a `POST /trucking` request to "Sol-3" (Earth).

#### Interaction Patterns in Use

- Resource Protection — The sellStationInventory loop is configured to liquidate all assets (Water, Food, etc.) while specifically protecting the iron_ore stock for the Earth dispatch mission.

- Space Elevator Logistics — Implements a periodic warehouse-to-orbit transfer. It identifies surplus stock in the planetary warehouse and uses the blocking POST /space-elevator pattern to move goods to the station.

- Error Handling & Resilience — Uses Project Reactor's onErrorResume and retryWhen operators to ensure the main pipelines (Market Scan, Trade Requests) stay alive even when the server returns 400 or 422 errors due to API mismatches.

#### Current Behavior Notes (Development State)

- Market Cold-Start: As the global market currently returns empty sets, the strategy focuses on placing initial liquidity orders at floor prices.

- Validation Constraints: Logistics are currently hard-wired to Sol-3 targets. Trade requests are currently strictly bound to iron_ore using the total variant to satisfy server-side requirements.

>>>>>>> 9b2d485aa087cef837107732182075d228359eb2
---di

## Interaction Patterns

| Pattern | Implementation |
|---|---|
| Sync REST | `ApiClient` — Reactor Netty `HttpClient`, non-blocking |
| Blocking elevator | `ElevatorService` — `subscribeOn(Schedulers.boundedElastic())`, 120s timeout |
| Polling | `ShipService.startPollingScheduler()`, `Flux.interval()` |
| SSE stream | `ApiClient.subscribeToTrades()` — Reactor Netty streaming, backpressure drop |
| Webhooks | `WebhookServer` (Vert.x) → `Sinks.Many` → reactive `Flux` |
