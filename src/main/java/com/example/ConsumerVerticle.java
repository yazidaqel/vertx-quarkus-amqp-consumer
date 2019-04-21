package com.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.ext.amqp.AmqpClient;
import io.vertx.ext.amqp.AmqpClientOptions;
import io.vertx.ext.amqp.AmqpConnection;
import io.vertx.ext.amqp.AmqpReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class ConsumerVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerVerticle.class);

    private AmqpClient client;

    @Override
    public void start() throws Exception {

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

        super.start();
    }

    @Override
    public void stop() throws Exception {
        client.close(x -> {
            if (x.succeeded()) {
                LOGGER.info("AmqpClient closed successfully");
            }
        });
        super.stop();
    }
}
