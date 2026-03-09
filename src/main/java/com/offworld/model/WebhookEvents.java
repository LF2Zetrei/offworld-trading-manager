package com.offworld.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Webhook event payloads sent by the server to our callback URL.
 */
public class WebhookEvents {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OriginDockingRequest(
            String event,
            @JsonProperty("ship_id") String shipId,
            @JsonProperty("origin_planet_id") String originPlanetId,
            @JsonProperty("destination_planet_id") String destinationPlanetId,
            Map<String, Integer> cargo) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DockingRequest(
            String event,
            @JsonProperty("ship_id") String shipId,
            @JsonProperty("origin_planet_id") String originPlanetId,
            Map<String, Integer> cargo) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ShipDocked(
            String event,
            @JsonProperty("ship_id") String shipId,
            String status) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ShipComplete(
            String event,
            @JsonProperty("ship_id") String shipId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConstructionComplete(
            String event,
            @JsonProperty("project_id") String projectId,
            @JsonProperty("project_type") String projectType,
            @JsonProperty("planet_id") String planetId) {}

    /**
     * Raw envelope to detect event type before deserializing the full payload.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EventEnvelope(String event) {}
}
