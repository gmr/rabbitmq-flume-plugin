package com.aweber.flume.source.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;


public class Consumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSource.class);
    volatile boolean shutdown = false;
    private String hostname;
    private int port;
    private boolean sslEnabled = false;
    private String virtualHost;
    private String username;
    private String password;
    private String queue;
    private boolean noAck = false;
    private int prefetchCount = 0;
    private int prefetchSize = 0;

    public Consumer() {
    }

    public String getHostname() {
        return hostname;
    }

    public Consumer setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public int getPort() {
        return port;
    }

    public Consumer setPort(int port) {
        this.port = port;
        return this;
    }

    public boolean isSSLEnabled() {
        return sslEnabled;
    }

    public Consumer setSSLEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public Consumer setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public Consumer setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Consumer setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public Consumer setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public boolean isNoAck() {
        return noAck;
    }

    public Consumer setNoAck(boolean noAck) {
        this.noAck = noAck;
        return this;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }

    public Consumer setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
        return this;
    }

    public int getPrefetchSize() {
        return prefetchSize;
    }

    public Consumer setPrefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
        return this;
    }

    @Override
    public void run() {
        Connection connection;
        Channel channel;

        ConnectionFactory factory = new ConnectionFactory();

        try {
            connection = createRabbitMQConnection(factory);
        } catch (IOException ex) {
            logger.error("Error creating RabbitMQ connection: {}", ex);
            return;
        }

        try {
            channel = connection.createChannel();
        } catch (IOException ex) {
            logger.error("Error creating RabbitMQ channel: {}", ex);
            return;
        }


        while (!shutdown) {

        }

        // Cancel consumer

        try {
            channel.close();
            connection.close();
        } catch (IOException ex) {
            logger.error("Error cleanly closing RabbitMQ connection: {}", ex.toString());
        }

    }

    public void shutdown() {
        shutdown = true;
    }

    private Connection createRabbitMQConnection(ConnectionFactory factory) throws IOException {
        factory.setHost(hostname);
        factory.setPort(port);
        factory.setVirtualHost(virtualHost);
        factory.setUsername(username);
        factory.setPassword(password);
        if (sslEnabled) {
            try {
                factory.useSslProtocol();
            } catch (NoSuchAlgorithmException e) {
                logger.error("Could not enable SSL: {}", e.toString());
            } catch (KeyManagementException e) {
                logger.error("Could not enable SSL: {}", e.toString());
            }
        }
        return factory.newConnection();
    }


}
