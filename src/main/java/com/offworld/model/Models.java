package com.offworld.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Map;

/**
 * All domain model records for the Offworld Trading Manager API.
 */
public final class Models {

    private Models() {}

    // ── Galaxy ────────────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Coordinates(double x, double y, double z) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PlanetType(String category, String climate) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Economy(
            long credits,
            @JsonProperty("tax_rate") double taxRate) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Settlement(
            String name,
            long population,
            Economy economy,
            @JsonProperty("founding_goods") Map<String, Long> foundingGoods) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MassDriver(@JsonProperty("max_channels") long maxChannels) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Station(
            String name,
            @JsonProperty("owner_id") String ownerId,
            Map<String, Long> inventory,
            @JsonProperty("mass_driver") MassDriver massDriver,
            @JsonProperty("docking_bays") long dockingBays,
            @JsonProperty("max_storage") @JsonDeserialize(using = SafeLongDeserializer.class) long maxStorage) {}

    static class SafeLongDeserializer extends com.fasterxml.jackson.databind.JsonDeserializer<Long> {
        @Override
        public Long deserialize(com.fasterxml.jackson.core.JsonParser p,
                                com.fasterxml.jackson.databind.DeserializationContext ctx)
                throws java.io.IOException {
            try {
                return p.getLongValue();
            } catch (Exception e) {
                // u64::MAX et autres valeurs hors range → Long.MAX_VALUE
                p.skipChildren();
                return Long.MAX_VALUE;
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElevatorConfig(
            @JsonProperty("cabin_count") long cabinCount,
            @JsonProperty("cabin_capacity") long cabinCapacity,
            @JsonProperty("transfer_duration_secs") long transferDurationSecs,
            @JsonProperty("failure_rate") double failureRate,
            @JsonProperty("repair_duration_secs") long repairDurationSecs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElevatorWarehouse(
            @JsonProperty("owner_id") String ownerId,
            Map<String, Long> inventory) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElevatorCabin(
            long id,
            String state,
            @JsonProperty("available_in_secs") Long availableInSecs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SpaceElevator(
            ElevatorWarehouse warehouse,
            ElevatorConfig config,
            List<ElevatorCabin> cabins) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(using = PlanetStatusDeserializer.class)
    public record PlanetStatus(
            String status,
            Settlement settlement,
            Station station,
            @JsonProperty("space_elevator") SpaceElevator spaceElevator) {}


    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Planet(
            String id,
            String name,
            long position,
            @JsonProperty("distance_ua") double distanceUa,
            @JsonProperty("planet_type") PlanetType planetType,
            String status,
            Settlement settlement,
            Station station,
            @JsonProperty("space_elevator") SpaceElevator spaceElevator) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StarSystem(
            String name,
            Coordinates coordinates,
            @JsonProperty("star_type") String starType,
            List<Planet> planets) {}

    static class PlanetStatusDeserializer extends com.fasterxml.jackson.databind.JsonDeserializer<Models.PlanetStatus> {
        @Override
        public Models.PlanetStatus deserialize(com.fasterxml.jackson.core.JsonParser p,
                                               com.fasterxml.jackson.databind.DeserializationContext ctx)
                throws java.io.IOException {
            com.fasterxml.jackson.databind.JsonNode node = p.getCodec().readTree(p);
            // Cas 1 : "status": "uninhabited" — simple string
            if (node.isTextual()) {
                return new Models.PlanetStatus(node.asText(), null, null, null);
            }
            // Cas 2 : objet complet avec settlement, station, etc.
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    (com.fasterxml.jackson.databind.ObjectMapper) p.getCodec();
            String status = node.has("status") ? node.get("status").asText() : null;
            Models.Settlement settlement = node.has("settlement") ?
                    mapper.treeToValue(node.get("settlement"), Models.Settlement.class) : null;
            Models.Station station = node.has("station") ?
                    mapper.treeToValue(node.get("station"), Models.Station.class) : null;
            Models.SpaceElevator elevator = node.has("space_elevator") ?
                    mapper.treeToValue(node.get("space_elevator"), Models.SpaceElevator.class) : null;
            return new Models.PlanetStatus(status, settlement, station, elevator);
        }
    }

    // ── Player ────────────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Player(
            String id,
            String name,
            long credits,
            @JsonProperty("api_key") String apiKey,
            @JsonProperty("callback_url") String callbackUrl,
            @JsonProperty("pulsar_biscuit") String pulsarBiscuit) {}

    // ── Market ────────────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MarketOrder(
            String id,
            @JsonProperty("player_id") String playerId,
            @JsonProperty("good_name") String goodName,
            String side,
            @JsonProperty("order_type") String orderType,
            double price,
            long quantity,
            @JsonProperty("filled_quantity") long filledQuantity,
            String status,
            @JsonProperty("station_planet_id") String stationPlanetId,
            @JsonProperty("created_at") long createdAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrderBookLevel(
            double price,
            @JsonProperty("total_quantity") long totalQuantity,
            @JsonProperty("order_count") long orderCount) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrderBook(
            @JsonProperty("good_name") String goodName,
            List<OrderBookLevel> bids,
            List<OrderBookLevel> asks,
            @JsonProperty("last_trade_price") Double lastTradePrice) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TradeEvent(
            String id,
            @JsonProperty("good_name") String goodName,
            double price,
            long quantity,
            @JsonProperty("buyer_id") String buyerId,
            @JsonProperty("seller_id") String sellerId,
            @JsonProperty("buyer_station") String buyerStation,
            @JsonProperty("seller_station") String sellerStation,
            long timestamp) {}

    // ── Shipping ──────────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Ship(
            String id,
            @JsonProperty("owner_id") String ownerId,
            @JsonProperty("origin_planet_id") String originPlanetId,
            @JsonProperty("destination_planet_id") String destinationPlanetId,
            Map<String, Long> cargo,
            String status,
            @JsonProperty("trade_id") String tradeId,
            @JsonProperty("trucking_id") String truckingId,
            long fee,
            @JsonProperty("created_at") long createdAt,
            @JsonProperty("arrival_at") Long arrivalAt,
            @JsonProperty("operation_complete_at") Long operationCompleteAt) {}

    // ── Space Elevator ────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TransferItem(
            @JsonProperty("good_name") String goodName,
            long quantity) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TransferResult(
            boolean success,
            @JsonProperty("cabin_id") long cabinId,
            @JsonProperty("duration_secs") long durationSecs,
            List<TransferItem> items,
            @JsonProperty("total_quantity") long totalQuantity,
            @JsonProperty("failure_reason") String failureReason) {}

    // ── Trade Requests ────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TradeRequest(
            String id,
            @JsonProperty("owner_id") String ownerId,
            @JsonProperty("planet_id") String planetId,
            @JsonProperty("good_name") String goodName,
            String direction,
            String mode,
            @JsonProperty("rate_per_tick") long ratePerTick,
            @JsonProperty("total_quantity") Long totalQuantity,
            @JsonProperty("target_level") Long targetLevel,
            @JsonProperty("cumulative_generated") long cumulativeGenerated,
            String status,
            @JsonProperty("created_at") long createdAt,
            @JsonProperty("completed_at") Long completedAt) {}

    // ── Construction ──────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConstructionProject(
            String id,
            @JsonProperty("owner_id") String ownerId,
            @JsonProperty("project_type") String projectType,
            @JsonProperty("source_planet_id") String sourcePlanetId,
            @JsonProperty("target_planet_id") String targetPlanetId,
            long fee,
            @JsonProperty("goods_consumed") Map<String, Long> goodsConsumed,
            @JsonProperty("extra_goods") Map<String, Long> extraGoods,
            String status,
            @JsonProperty("created_at") long createdAt,
            @JsonProperty("completion_at") long completionAt,
            @JsonProperty("station_name") String stationName,
            @JsonProperty("settlement_name") String settlementName) {}

    // ── Leaderboard ───────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LeaderboardEntry(
            @JsonProperty("player_id") String playerId,
            @JsonProperty("player_name") String playerName,
            long profit) {}

    // ── Request DTOs ──────────────────────────────────────────────────────────

    public record PlaceOrderRequest(
            @JsonProperty("good_name") String goodName,
            String side,
            @JsonProperty("order_type") String orderType,
            Double price,
            long quantity,
            @JsonProperty("station_planet_id") String stationPlanetId) {}

    public record TruckingRequest(
            @JsonProperty("origin_planet_id") String originPlanetId,
            @JsonProperty("destination_planet_id") String destinationPlanetId,
            Map<String, Long> cargo) {}

    public record TransferRequest(String direction, List<TransferItem> items) {}

    public record TradeRequestCreate(
            @JsonProperty("planet_id") String planetId,
            @JsonProperty("good_name") String goodName,
            String direction,
            String mode,
            @JsonProperty("rate_per_tick") long ratePerTick,
            @JsonProperty("total_quantity") Long totalQuantity,
            @JsonProperty("target_level") Long targetLevel) {}

    public record UpdatePlayerRequest(
            String name,
            @JsonProperty("callback_url") String callbackUrl) {}

    public record UpgradeStationRequest(
            @JsonProperty("planet_id") String planetId,
            @JsonProperty("upgrade_type") String upgradeType) {}
}
