package com.example;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import io.vertx.ext.amqp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

@ApplicationScoped
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    @Inject
    private Vertx vertx;

    private String deploymentId;

    void onStart(@Observes StartupEvent ev) {
        LOGGER.info("onStart");
        startAmqpConnection();
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOGGER.info("onStop");
        stopAmqpConnection();
    }

    private void startAmqpConnection() {
        ConsumerVerticle consumerVerticle = new ConsumerVerticle();
        vertx.deployVerticle(consumerVerticle, handler ->{
            if(handler.succeeded()){
                deploymentId = handler.result();
            }
        });
    }

    private void stopAmqpConnection() {
        vertx.undeploy(deploymentId, handler->{
            if(handler.succeeded()){
                vertx.close();
            }
        });
    }
}
