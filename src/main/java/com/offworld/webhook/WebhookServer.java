package com.offworld.webhook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.offworld.config.AppConfig;
import com.offworld.model.WebhookEvents;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Lightweight Vert.x HTTP server that receives webhook callbacks from the game server.
 *
 * <p>Incoming POST /webhooks events are pushed into reactive Sinks so downstream
 * consumers can react using standard Flux operators.
 */
public class WebhookServer {
    private static final Logger log = LoggerFactory.getLogger(WebhookServer.class);

    private final AppConfig config;
    private final ObjectMapper mapper;

    // Hot sinks — events are multicast to all subscribers
    private final Sinks.Many<WebhookEvents.OriginDockingRequest> originDockingSink =
            Sinks.many().multicast().onBackpressureBuffer(256);
    private final Sinks.Many<WebhookEvents.DockingRequest> dockingSink =
            Sinks.many().multicast().onBackpressureBuffer(256);
    private final Sinks.Many<WebhookEvents.ShipDocked> shipDockedSink =
            Sinks.many().multicast().onBackpressureBuffer(256);
    private final Sinks.Many<WebhookEvents.ShipComplete> shipCompleteSink =
            Sinks.many().multicast().onBackpressureBuffer(256);
    private final Sinks.Many<WebhookEvents.ConstructionComplete> constructionCompleteSink =
            Sinks.many().multicast().onBackpressureBuffer(64);

    private HttpServer server;

    public WebhookServer(AppConfig config, ObjectMapper mapper) {
        this.config = config;
        this.mapper = mapper;
    }

    public void start() {
        Vertx vertx = Vertx.vertx();
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.post("/webhooks").handler(ctx -> {
            String body = ctx.body().asString();
            log.debug("Webhook received: {}", body);
            try {
                WebhookEvents.EventEnvelope envelope =
                        mapper.readValue(body, WebhookEvents.EventEnvelope.class);
                dispatchEvent(envelope.event(), body);
                ctx.response().setStatusCode(200).end("OK");
            } catch (Exception e) {
                log.error("Failed to parse webhook: {}", body, e);
                ctx.response().setStatusCode(400).end("Bad Request");
            }
        });

        server = vertx.createHttpServer()
                .requestHandler(router)
                .listen(config.getWebhookPort(), config.getWebhookHost(), res -> {
                    if (res.succeeded()) {
                        log.info("Webhook server listening on {}:{}", config.getWebhookHost(), config.getWebhookPort());
                    } else {
                        log.error("Failed to start webhook server", res.cause());
                    }
                });
    }

    private void dispatchEvent(String event, String body) throws Exception {
        switch (event) {
            case "OriginDockingRequest" -> {
                var e = mapper.readValue(body, WebhookEvents.OriginDockingRequest.class);
                log.info("Ship {} arrived at origin {}", e.shipId(), e.originPlanetId());
                originDockingSink.tryEmitNext(e);
            }
            case "DockingRequest" -> {
                var e = mapper.readValue(body, WebhookEvents.DockingRequest.class);
                log.info("Ship {} arrived at destination", e.shipId());
                dockingSink.tryEmitNext(e);
            }
            case "ShipDocked" -> {
                var e = mapper.readValue(body, WebhookEvents.ShipDocked.class);
                log.info("Ship {} docked, status={}", e.shipId(), e.status());
                shipDockedSink.tryEmitNext(e);
            }
            case "ShipComplete" -> {
                var e = mapper.readValue(body, WebhookEvents.ShipComplete.class);
                log.info("Ship {} delivery complete", e.shipId());
                shipCompleteSink.tryEmitNext(e);
            }
            case "ConstructionComplete" -> {
                var e = mapper.readValue(body, WebhookEvents.ConstructionComplete.class);
                log.info("Construction project {} complete", e.projectId());
                constructionCompleteSink.tryEmitNext(e);
            }
            default -> log.warn("Unknown webhook event type: {}", event);
        }
    }

    public void stop() {
        if (server != null) server.close();
    }

    // ── Public Flux accessors ─────────────────────────────────────────────────

    public Flux<WebhookEvents.OriginDockingRequest> originDockingRequests() {
        return originDockingSink.asFlux();
    }

    public Flux<WebhookEvents.DockingRequest> dockingRequests() {
        return dockingSink.asFlux();
    }

    public Flux<WebhookEvents.ShipDocked> shipDockedEvents() {
        return shipDockedSink.asFlux();
    }

    public Flux<WebhookEvents.ShipComplete> shipCompleteEvents() {
        return shipCompleteSink.asFlux();
    }

    public Flux<WebhookEvents.ConstructionComplete> constructionCompleteEvents() {
        return constructionCompleteSink.asFlux();
    }
}
