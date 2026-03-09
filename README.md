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

The strategy runs three parallel loops:
- **SSE reaction** — updates price history on every trade event
- **Market scan** — every 10s, checks order books for profitable spreads and places limit buy+sell pairs
- **Trade requests** — seeds standing export requests to maintain supply

---

## Interaction Patterns

| Pattern | Implementation |
|---|---|
| Sync REST | `ApiClient` — Reactor Netty `HttpClient`, non-blocking |
| Blocking elevator | `ElevatorService` — `subscribeOn(Schedulers.boundedElastic())`, 120s timeout |
| Polling | `ShipService.startPollingScheduler()`, `Flux.interval()` |
| SSE stream | `ApiClient.subscribeToTrades()` — Reactor Netty streaming, backpressure drop |
| Webhooks | `WebhookServer` (Vert.x) → `Sinks.Many` → reactive `Flux` |
