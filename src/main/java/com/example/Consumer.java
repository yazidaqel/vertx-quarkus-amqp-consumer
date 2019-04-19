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

    private AmqpClient client;

    void onStart(@Observes StartupEvent ev) {
        LOGGER.info("onStart");
        startAmqpConnection();
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOGGER.info("onStop");
        stopAmqpConnection();
    }

    private void startAmqpConnection() {
        AmqpClientOptions options = new AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672);
        // Create a client using its own internal Vert.x instance.
        //AmqpClient client = AmqpClient.create(options);

        // USe an explicit Vert.x instance.
        client = AmqpClient.create(vertx, options);

        client.connect(ar -> {
            if (ar.failed()) {
                LOGGER.info("Unable to connect to the broker");
            } else {
                LOGGER.info("Connection succeeded");
                AmqpConnection connection = ar.result();
                connection.createReceiver("my-queue", done -> {
                    if (done.failed()) {
                        LOGGER.info("Unable to create a sender");
                    } else {
                        AmqpReceiver receiver = done.result();
                        receiver
                                .exceptionHandler(t -> {
                                    LOGGER.error(t.getMessage());
                                })
                                .handler(msg -> {
                                    LOGGER.info(msg.bodyAsString());
                                });
                    }
                });
            }
        });
    }

    private void stopAmqpConnection() {
        client.close(x -> {
            if (x.succeeded()) {
                vertx.close();
            }
        });
    }
}
