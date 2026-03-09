package com.offworld.service;

import com.offworld.api.ApiClient;
import com.offworld.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import com.offworld.model.Models.*;

/**
 * Builds and maintains a reactive in-memory model of the galaxy.
 * Parallel exploration of all systems using Flux.merge + flatMap.
 */
public class GalaxyService {
    private static final Logger log = LoggerFactory.getLogger(GalaxyService.class);

    private final ApiClient api;

    // In-memory cache
    private final Map<String, StarSystem> systems = new ConcurrentHashMap<>();
    private final Map<String, Planet> connectedPlanets = new ConcurrentHashMap<>();   // planetId -> Planet

    public GalaxyService(ApiClient api) {
        this.api = api;
    }

    /**
     * Explore the entire galaxy: fetch all systems and their planets in parallel.
     * Returns a Flux of all connected planets (those with stations).
     */
    public Flux<Planet> exploreGalaxy() {
        log.info("Starting galaxy exploration...");
        return api.getSystems()
                .flatMapMany(Flux::fromIterable)
                .flatMap(system -> api.getSystem(system.name())
                        .doOnNext(s -> {
                            systems.put(s.name(), s);
                            log.debug("Discovered system: {} ({} planets)", s.name(),
                                    s.planets() != null ? s.planets().size() : 0);
                        })
                        .flatMapMany(s -> Flux.fromIterable(
                                s.planets() != null ? s.planets() : List.of()
                        )))
                .doOnNext(planet -> log.info("RAW planet: {} status={} station={}",
                        planet.name(), planet.status(), planet.station()))
                .filter(planet -> "connected".equals(planet.status()))
                .doOnNext(planet -> {
                    connectedPlanets.put(planet.id(), planet);
                    log.info("Connected planet: {} [{}]", planet.name(), planet.id());
                })
                .doOnComplete(() -> log.info("Galaxy exploration complete. {} connected planets found.",
                        connectedPlanets.size()))
                .doOnError(e -> log.error("Galaxy exploration error", e))
                .retryWhen(reactor.util.retry.Retry.fixedDelay(3, Duration.ofSeconds(5)));
    }

    /**
     * Get all connected planets that have supply of a given good.
     */
    public List<Planet> getPlanetsBySupply(String goodName) {
        return new ArrayList<>(connectedPlanets.values());
    }

    /**
     * Get all connected planets that have demand for a given good.
     */
    public List<Planet> getPlanetsByDemand(String goodName) {
        return new ArrayList<>(connectedPlanets.values());
    }

    public Map<String, Planet> getConnectedPlanets() { return connectedPlanets; }
    public Map<String, StarSystem> getSystems() { return systems; }

    /**
     * Find the system name for a given planet ID.
     */
    public String getSystemForPlanet(String planetId) {
        return systems.values().stream()
                .filter(s -> s.planets() != null &&
                             s.planets().stream().anyMatch(p -> p.id().equals(planetId)))
                .map(StarSystem::name)
                .findFirst()
                .orElse(null);
    }

    /**
     * Refresh a single planet's status.
     */
    public Mono<Planet> refreshPlanet(String planetId) {
        String system = getSystemForPlanet(planetId);
        if (system == null) return Mono.error(new IllegalArgumentException("Unknown planet: " + planetId));
        return api.getPlanet(system, planetId)
                .doOnNext(p -> connectedPlanets.put(p.id(), p));
    }
}
