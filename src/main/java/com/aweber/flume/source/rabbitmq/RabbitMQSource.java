/**
 * RabbitMQ Source Plugin for Flume
 */

package com.aweber.flume.source.rabbitmq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;

// RabbitMQ Imports
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;

// Flume Imports
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;

import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.SimpleEvent;

import org.apache.flume.source.AbstractSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RabbitMQSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSource.class);

    private Channel channel;
    private Connection connection;
    private ConnectionFactory connectionFactory;
    private CounterGroup counterGroup;

    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";
    private static final String SSL_KEY = "ssl";
    private static final String VHOST_KEY = "virtual-host";
    private static final String USER_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String QUEUE_KEY = "queue";
    private static final String EXCLUDE_PROTOCOLS = "exclude-protocols";
    private static final String NOACK_KEY = "queue";
    private static final String PREFETCH_COUNT_KEY = "prefetch-count";
    private static final String PREFETCH_SIZE_KEY = "prefetch-size";
    private static final String THREAD_COUNT_KEY = "threads";
    private static final String SSLv3 = "SSLv3";

    private String hostname;
    private int port;
    private boolean enableSSL = false;
    private String virtualHost;
    private String username;
    private String password;
    private String queue;
    private boolean noAck = false;
    private int prefetchCount = 0;
    private int prefetchSize = 0;
    private int consumerThreads = 1;

    private final List<String> excludeProtocols = new LinkedList<String>();

    public RabbitMQSource(){
        connectionFactory = new ConnectionFactory();
        counterGroup = new CounterGroup();
    }

    @Override
    public void configure(Context context) {

        // Only the queue name does not have a default value
        Configurables.ensureRequiredNonNull(context, QUEUE_KEY);

        // Assign all of the configured values
        hostname = context.getString(HOST_KEY, "localhost");
        port = context.getInteger(PORT_KEY, 5672);
        enableSSL = context.getBoolean(SSL_KEY, false);
        virtualHost = context.getString(VHOST_KEY, "/");
        username = context.getString(USER_KEY, "guest");
        password = context.getString(PASSWORD_KEY, "guest");
        queue = context.getString(QUEUE_KEY, null);
        noAck = context.getBoolean(NOACK_KEY, false);
        prefetchCount = context.getInteger(PREFETCH_COUNT_KEY, 0);
        prefetchSize = context.getInteger(PREFETCH_SIZE_KEY, 0);
        consumerThreads = context.getInteger(THREAD_COUNT_KEY, 1);

        // Get any specified protocols to exclude
        excludeProtocols.clear();
        String tmp = context.getString(EXCLUDE_PROTOCOLS, "");
        if (!tmp.isEmpty()) {
            excludeProtocols.addAll(Arrays.asList(tmp.split(" ")));
        }

        // Add SSLv3 if missing
        if (!excludeProtocols.contains(SSLv3)) {
            excludeProtocols.add(SSLv3);
        }
    }

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", this);

        // Connect to RabbitMQ

        // Open a Channel

        // Verify the queue exists with a passive declare

        // Build the thread for processing messages


        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping {}...", this);

        // Cancel consumer

        // Stop consumer thread

        // Close channel

        // Close connection

        super.stop();
    }
}
