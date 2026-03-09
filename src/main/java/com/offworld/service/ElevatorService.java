package com.offworld.service;

import com.offworld.api.ApiClient;
import com.offworld.config.AppConfig;
import com.offworld.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import com.offworld.model.Models.*;

/**
 * Manages space elevator transfers.
 *
 * <p>The elevator transfer endpoint blocks the HTTP connection for several seconds.
 * We subscribe on a bounded-elastic thread (not the Netty event loop) so that
 * the blocking I/O does not stall other reactive pipelines.
 *
 * <p>On cabin malfunction the transfer returns success=false; we retry automatically.
 */
public class ElevatorService {
    private static final Logger log = LoggerFactory.getLogger(ElevatorService.class);

    private final ApiClient api;
    private final AppConfig config;

    public ElevatorService(ApiClient api, AppConfig config) {
        this.api = api;
        this.config = config;
    }

    /**
     * Transfer goods to the surface, retrying on cabin failures.
     *
     * @param systemName  star system name
     * @param planetId    planet ID
     * @param items       list of goods to transfer
     */
    public Mono<TransferResult> transferToSurface(String systemName, String planetId,
                                                   List<TransferItem> items) {
        return doTransfer(systemName, planetId, "to_surface", items);
    }

    /**
     * Transfer goods to orbit (station), retrying on cabin failures.
     */
    public Mono<TransferResult> transferToOrbit(String systemName, String planetId,
                                                 List<TransferItem> items) {
        return doTransfer(systemName, planetId, "to_orbit", items);
    }

    private Mono<TransferResult> doTransfer(String systemName, String planetId,
                                             String direction, List<TransferItem> items) {
        TransferRequest request = new TransferRequest(direction, items);

        return api.transferElevator(systemName, planetId, request)
                // The actual HTTP call may block; offload to bounded-elastic to avoid
                // blocking the Netty event loop
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(result -> {
                    if (!result.success()) {
                        log.warn("Elevator transfer failed (cabin malfunction): {}. Retrying...",
                                result.failureReason());
                        // Return an error so retry kicks in
                        return Mono.error(new ElevatorFailureException(result.failureReason()));
                    }
                    log.info("Elevator transfer {} complete: {} units", direction, result.totalQuantity());
                    return Mono.just(result);
                })
                .retryWhen(reactor.util.retry.Retry
                        .fixedDelay(config.getElevatorRetryAttempts(),
                                    Duration.ofMillis(config.getElevatorRetryDelayMs()))
                        .filter(e -> e instanceof ElevatorFailureException)
                        .doAfterRetry(rs -> log.info("Elevator retry attempt {}", rs.totalRetries() + 1)))
                .doOnError(e -> log.error("Elevator transfer {} on {} failed after retries", direction, planetId, e));
    }

    /** Splits a large transfer into cabin-capacity chunks and runs them sequentially. */
    public Mono<Void> transferInChunks(String systemName, String planetId, String direction,
                                       String goodName, int totalQty, int cabinCapacity) {
        if (totalQty <= 0) return Mono.empty();

        int chunks = (int) Math.ceil((double) totalQty / cabinCapacity);
        Mono<Void> chain = Mono.empty();
        for (int i = 0; i < chunks; i++) {
            int qty = Math.min(cabinCapacity, totalQty - i * cabinCapacity);
            List<TransferItem> items = List.of(new TransferItem(goodName, qty));
            chain = chain.then(doTransfer(systemName, planetId, direction, items).then());
        }
        return chain;
    }

    static class ElevatorFailureException extends RuntimeException {
        ElevatorFailureException(String reason) { super(reason); }
    }
}
