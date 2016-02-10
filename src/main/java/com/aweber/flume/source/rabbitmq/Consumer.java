package com.aweber.flume.source.rabbitmq;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private static final String COUNTER_ACK = "rabbitmq.ack";
    private static final String COUNTER_EXCEPTION = "rabbitmq.exception";
    private static final String COUNTER_REJECT = "rabbitmq.reject";

    volatile boolean shutdown = false;
    private Connection connection;
    private Channel channel;
    private ChannelProcessor channelProcessor;
    private CounterGroup counterGroup;
    private SourceCounter sourceCounter;

    private String hostname;
    private int port;
    private boolean sslEnabled = false;
    private String virtualHost;
    private String username;
    private String password;
    private String queue;
    private boolean autoAck = false;
    private int prefetchCount = 0;
    private int timeout = -1;

    public Consumer() {
    }

    public Consumer setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public Consumer setPort(int port) {
        this.port = port;
        return this;
    }

    public Consumer setSSLEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public Consumer setChannelProcessor(ChannelProcessor channelProcessor) {
        this.channelProcessor = channelProcessor;
        return this;
    }

    public Consumer setCounterGroup(CounterGroup counterGroup) {
        this.counterGroup = counterGroup;
        return this;
    }

    public Consumer setSourceCounter(SourceCounter sourceCounter) {
        this.sourceCounter = sourceCounter;
        return this;
    }

    public Consumer setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return this;
    }

    public Consumer setUsername(String username) {
        this.username = username;
        return this;
    }

    public Consumer setPassword(String password) {
        this.password = password;
        return this;
    }

    public Consumer setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public Consumer setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
        return this;
    }

    public Consumer setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
        return this;
    }

    public Consumer setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public void run() {
        QueueingConsumer consumer;
        QueueingConsumer.Delivery delivery;

        ConnectionFactory factory = new ConnectionFactory();

        // Connect to RabbitMQ
        try {
            connection = createRabbitMQConnection(factory);
        } catch (IOException ex) {
            logger.error("Error creating RabbitMQ connection: {}", ex);
            return;
        }

        // Keep track of how many connections were opened
        sourceCounter.setOpenConnectionCount(sourceCounter.getOpenConnectionCount() + 1);

        // Open the channel
        try {
            channel = connection.createChannel();
        } catch (IOException ex) {
            logger.error("Error creating RabbitMQ channel: {}", ex);
            return;
        }

        // Set QoS Prefetching if enabled, exiting if it fails
        if (prefetchCount > 0) {
            if (!setQoS()) {
                this.close();
                return;
            }
        }

        // Create the new consumer and set the consumer tag
        consumer = new QueueingConsumer(channel);

        try {
            channel.basicConsume(queue, autoAck, consumer);
        } catch (IOException ex) {
            logger.error("Error starting consumer: {}", ex);
            counterGroup.incrementAndGet(COUNTER_EXCEPTION);
            this.close();
            return;
        }

        // Loop until shutdown is called
        while (!shutdown) {

            // Get the next message from the stack
            try {
                // Handle timeout
                if (timeout < 0) {
                    delivery = consumer.nextDelivery();
                } else {
                    delivery = consumer.nextDelivery(timeout); // returns null on timeout
                }
            } catch (InterruptedException ex) {
                logger.error("Consumer interrupted for {}, exiting: {}", this, ex);
                break;
            }
            // Process the delivery if any
            if (delivery != null) {
                sourceCounter.incrementEventReceivedCount();

                try {
                    channelProcessor.processEvent(parseMessage(delivery));
                } catch (Exception ex) {
                    logger.error("Error writing to channel for {}, message rejected {}", this, ex);
                    rejectMessage(getDeliveryTag(delivery));
                    continue;
                }
                sourceCounter.incrementEventAcceptedCount();
                if (!autoAck) ackMessage(getDeliveryTag(delivery));
            }
        }

        // Tell RabbitMQ that the consumer is stopping
        cancelConsumer(consumer.getConsumerTag());

        // Cancel consumer
        this.close();
    }

    private long getDeliveryTag(QueueingConsumer.Delivery delivery) {
        return delivery.getEnvelope().getDeliveryTag();
    }

    private void cancelConsumer(String consumerTag) {
        try {
            channel.basicCancel(consumerTag);
        } catch (IOException ex) {
            logger.error("Error cancelling consumer for {}: {}", this, ex);
            counterGroup.incrementAndGet(COUNTER_EXCEPTION);
        }
    }

    private void ackMessage(long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (IOException ex) {
            logger.error("Error acknowledging message from {}: {}", this, ex);
            counterGroup.incrementAndGet(COUNTER_EXCEPTION);
        }
        counterGroup.incrementAndGet(COUNTER_ACK);
    }

    private void rejectMessage(long deliveryTag) {
        try {
            channel.basicReject(deliveryTag, false);
        } catch (IOException ex) {
            logger.error("Error rejecting message from {}: {}", this, ex);
            counterGroup.incrementAndGet(COUNTER_EXCEPTION);
        }
        counterGroup.incrementAndGet(COUNTER_REJECT);
    }

    private Event parseMessage(QueueingConsumer.Delivery delivery) {
        // Create the event passing in the body
        Event event = EventBuilder.withBody(delivery.getBody());

        // Get the headers from properties, exchange, and routing-key
        Map<String, String> headers = buildHeaders(delivery.getProperties());

        String exchange = delivery.getEnvelope().getExchange();
        if (exchange != null && !exchange.isEmpty()) {
            headers.put("exchange", exchange);
        }

        String routingKey = delivery.getEnvelope().getRoutingKey();
        if (routingKey != null && !routingKey.isEmpty()) {
            headers.put("routing-key", routingKey);
        }

        event.setHeaders(headers);
        return event;
    }

    private Map<String, String> buildHeaders(AMQP.BasicProperties props) {
        Map<String, String> headers = new HashMap<String, String>();

        String appId = props.getAppId();
        String contentEncoding = props.getContentEncoding();
        String contentType = props.getContentType();
        String correlationId = props.getCorrelationId();
        Integer deliveryMode = props.getDeliveryMode();
        String expiration = props.getExpiration();
        String messageId = props.getMessageId();
        Integer priority = props.getPriority();
        String replyTo = props.getReplyTo();
        Date timestamp = props.getTimestamp();
        String type = props.getType();
        String userId = props.getUserId();

        if (appId != null && !appId.isEmpty()) {
            headers.put("app-id", appId);
        }
        if (contentEncoding != null && !contentEncoding.isEmpty()) {
            headers.put("content-encoding", contentEncoding);
        }
        if (contentType != null && !contentType.isEmpty()) {
            headers.put("content-type", contentType);
        }
        if (correlationId != null && !correlationId.isEmpty()) {
            headers.put("correlation-id", correlationId);
        }
        if (deliveryMode != null) {
            headers.put("delivery-mode", String.valueOf(deliveryMode));
        }
        if (expiration != null && !expiration.isEmpty()) {
            headers.put("expiration", expiration);
        }
        if (messageId != null && !messageId.isEmpty()) {
            headers.put("message-id", messageId);
        }
        if (priority != null) {
            headers.put("priority", String.valueOf(priority));
        }
        if (replyTo != null && !replyTo.isEmpty()) {
            headers.put("replyTo", replyTo);
        }
        if (timestamp != null) {
            headers.put("timestamp", String.valueOf(timestamp.getTime()));
        }
        if (type != null && !type.isEmpty()) {
            headers.put("type", type);
        }
        if (userId != null && !userId.isEmpty()) {
            headers.put("user-id", userId);
        }

        return headers;
    }

    private boolean setQoS() {
        try {
            channel.basicQos(prefetchCount);
        } catch (IOException ex) {
            logger.error("Error setting QoS prefetching: {}", ex);
            return false;
        }
        return true;
    }

    public void shutdown() {
        shutdown = true;
    }

    private void close() {
        try {
            channel.close();
            connection.close();
        } catch (IOException ex) {
            logger.error("Error cleanly closing RabbitMQ connection: {}", ex.toString());
        }
    }

    private Connection createRabbitMQConnection(ConnectionFactory factory) throws IOException {
        logger.debug("Connecting to RabbitMQ from {}", this);
        factory.setAutomaticRecoveryEnabled(true);
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
