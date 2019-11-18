package com.aweber.flume.sink.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Map;

public class RabbitMQSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSink.class);
    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";
    private static final String SSL_KEY = "ssl";
    private static final String VHOST_KEY = "virtual-host";
    private static final String USER_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String EXCHANGE_KEY = "exchange";
    private static final String ROUTING_KEY = "routing-key";
    private static final String AUTO_PROPERTIES_KEY = "auto-properties";
    private static final String MANDATORY_PUBLISH_KEY = "mandatory-publish";
    private static final String PUBLISHER_CONFIRMS_KEY = "publisher-confirms";
    private static final String APP_ID_KEY = "app-id";
    private static final String CONTENT_ENCODING_KEY = "content-encoding";
    private static final String CONTENT_TYPE_KEY = "content-type";
    private static final String DELIVERY_MODE_KEY = "delivery-mode";
    private static final String EXPIRES_KEY = "expires";
    private static final String CORRELATION_ID_KEY = "correlation-id";
    private static final String ID_KEY = "id";
    private static final String MESSAGE_ID_KEY = "message-id";
    private static final String PRIORITY_KEY = "priority";
    private static final String REPLY_TO_KEY = "reply_to";
    private static final String TIMESTAMP_KEY = "timestamp";
    private static final String TYPE_KEY = "type";
    private static final String USER_ID_KEY = "user-id";

    private static final String DEFAULT_EXCHANGE = "amq.topic";
    private static final String DEFAULT_ROUTING_KEY = "";

    private static final String EVENT_EMPTY = "event.empty";
    private static final String EVENT_RECEIVED = "event.empty";
    private static final String EVENT_PUBLISHED = "event.empty";
    private static final String EXCEPTION_CHANNEL = "exception.channel";
    private static final String EXCEPTION_DELIVERY = "exception.delivery";
    private static final String RABBITMQ_CLOSED = "rabbitmq.closed";
    private static final String RABBITMQ_CONNECTED = "rabbitmq.connected";
    private static final String RABBITMQ_EXCEPTION_SSL = "rabbitmq.exception.ssl";
    private static final String RABBITMQ_EXCEPTION_CONNECTION = "rabbitmq.exception.connection";

    private String exchange;
    private String routingKey;
    private Boolean autoProperties;
    private Boolean mandatory;
    private Boolean publisherConfirms;
    private CounterGroup counterGroup;
    private String hostname;
    private int port;
    private boolean sslEnabled = false;
    private String virtualHost;
    private String username;
    private String password;
    private com.rabbitmq.client.Channel rmqChannel = null;
    private com.rabbitmq.client.Connection connection = null;
    private com.rabbitmq.client.ConnectionFactory factory;

    public RabbitMQSink() {
        this(new ConnectionFactory());
    }

    public RabbitMQSink(ConnectionFactory factory) {
        this.factory = factory;
    }

    @Override
    public void configure(Context context) {
        hostname = context.getString(HOST_KEY, ConnectionFactory.DEFAULT_HOST);
        port = context.getInteger(PORT_KEY, ConnectionFactory.DEFAULT_AMQP_PORT);
        sslEnabled = context.getBoolean(SSL_KEY, false);
        virtualHost = context.getString(VHOST_KEY, ConnectionFactory.DEFAULT_VHOST);
        username = context.getString(USER_KEY, ConnectionFactory.DEFAULT_USER);
        password = context.getString(PASSWORD_KEY, ConnectionFactory.DEFAULT_PASS);
        exchange = context.getString(EXCHANGE_KEY, DEFAULT_EXCHANGE);
        routingKey = context.getString(ROUTING_KEY, DEFAULT_ROUTING_KEY);
        autoProperties = context.getBoolean(AUTO_PROPERTIES_KEY, true);
        mandatory = context.getBoolean(MANDATORY_PUBLISH_KEY, false);
        publisherConfirms = context.getBoolean(PUBLISHER_CONFIRMS_KEY, false);
        counterGroup = new CounterGroup();
        counterGroup.setName(getName());
    }

    @Override
    public Status process() throws EventDeliveryException {

        maybeConnectToRabbitMQ();

        Status status = Status.READY;
        Transaction transaction = getTransaction();

        try {
            transaction.begin();
            Event event = getChannel().take();

            if (event == null) {
                counterGroup.incrementAndGet(EVENT_EMPTY);
                status = Status.BACKOFF;
            } else {
                counterGroup.incrementAndGet(EVENT_RECEIVED);
                publishMessage(event);
                counterGroup.incrementAndGet(EVENT_PUBLISHED);

            }
            transaction.commit();

        } catch (ChannelException ex) {
            counterGroup.incrementAndGet(EXCEPTION_CHANNEL);
            transaction.rollback();
            status = Status.BACKOFF;
            logger.error("Unable to get event from channel. Exception follows.", ex);

        } catch (EventDeliveryException ex) {
            counterGroup.incrementAndGet(EXCEPTION_DELIVERY);
            transaction.rollback();
            status = Status.BACKOFF;
            logger.error("Delivery exception: {}", ex);

        } finally {
            transaction.close();
        }

        return status;
    }

    @Override
    public synchronized void start() {
        logger.info("Starting RabbitMQ Sink {}", this.getName());
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping RabbitMQ Sink {}", this.getName());
        if (connection != null) {
            closeRabbitMQConnection();
        }
        logger.info("RabbitMQ sink {} stopped. Metrics: {}", this.getName(), counterGroup.getCounters());
        super.stop();
    }


    private void closeRabbitMQConnection() {
        if (rmqChannel != null) {
            try {
                rmqChannel.close();
            } catch (IOException ex) {
                logger.error("Could not close the RabbitMQ Channel: {}", ex.toString());
            }
            catch (Exception ex) {
                throw new IllegalArgumentException("WTF Could not connect to RabbitMQ: General exception - " + ex.toString());
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException ex) {
                logger.error("Could not close the RabbitMQ Connection: {}", ex.toString());
            }
        }
        rmqChannel = null;
        connection = null;
        counterGroup.incrementAndGet(RABBITMQ_CLOSED);
    }

    private Channel createRabbitMQChannel() throws EventDeliveryException {
        try {
            return connection.createChannel();
        } catch (IOException ex) {
            closeRabbitMQConnection();
            throw new EventDeliveryException(ex.toString());
        }
    }

    private Connection createRabbitMQConnection(ConnectionFactory factory) throws EventDeliveryException {
        logger.debug("Connecting to RabbitMQ from {}", this.getName());
        counterGroup.incrementAndGet(RABBITMQ_CONNECTED);
        factory.setHost(hostname);
        factory.setPort(port);
        factory.setVirtualHost(virtualHost);
        factory.setUsername(username);
        factory.setPassword(password);
        if (sslEnabled) {
            try {
                factory.useSslProtocol();
            } catch (NoSuchAlgorithmException ex) {
                counterGroup.incrementAndGet(RABBITMQ_EXCEPTION_SSL);
                logger.error("Could not enable SSL: {}", ex.toString());
                throw new EventDeliveryException("Could not Enable SSL: " + ex.toString());
            } catch (KeyManagementException ex) {
                counterGroup.incrementAndGet(RABBITMQ_EXCEPTION_SSL);
                logger.error("Could not enable SSL: {}", ex.toString());
                throw new EventDeliveryException("Could not Enable SSL: " + ex.toString());
            }
        }
        try {
            return factory.newConnection();
        } catch (IOException ex) {
            counterGroup.incrementAndGet(RABBITMQ_EXCEPTION_CONNECTION);
            throw new EventDeliveryException(ex.toString());
        }
        catch (Exception ex) {
            throw new IllegalArgumentException("Sink: Could not connect to RabbitMQ: General exception - " + ex.toString());
        }
    }

    private AMQP.BasicProperties createProperties(Map<String, String> headers) {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        if (autoProperties) {

            if (headers.containsKey(APP_ID_KEY)) {
                builder.appId(headers.get(APP_ID_KEY));
            } else {
                builder.appId(this.getName());
            }

            if (headers.containsKey(CONTENT_ENCODING_KEY)) {
                builder.contentEncoding(headers.get(CONTENT_ENCODING_KEY));
            }

            if (headers.containsKey(CONTENT_TYPE_KEY)) {
                builder.contentType(headers.get(CONTENT_TYPE_KEY));
            }

            if (headers.containsKey(CORRELATION_ID_KEY)) {
                builder.correlationId(headers.get(CORRELATION_ID_KEY));
            }

            if (headers.containsKey(DELIVERY_MODE_KEY)) {
                builder.deliveryMode(Integer.parseInt(headers.get(DELIVERY_MODE_KEY)));
            }

            if (headers.containsKey(EXPIRES_KEY)) {
                builder.expiration(headers.get(EXPIRES_KEY));
            }

            if (headers.containsKey(MESSAGE_ID_KEY)) {
                builder.messageId(headers.get(MESSAGE_ID_KEY));
            } else if (headers.containsKey(ID_KEY)) {
                builder.messageId(headers.get(ID_KEY));
            }

            if (headers.containsKey(PRIORITY_KEY)) {
                builder.priority(Integer.parseInt(headers.get(PRIORITY_KEY)));
            }

            if (headers.containsKey(REPLY_TO_KEY)) {
                builder.replyTo(headers.get(REPLY_TO_KEY));
            }

            if (headers.containsKey(TIMESTAMP_KEY)) {
                builder.timestamp(new Date(Long.parseLong(headers.get(TIMESTAMP_KEY))));
            } else {
                builder.timestamp(new Date());
            }

            if (headers.containsKey(TYPE_KEY)) {
                builder.replyTo(headers.get(TYPE_KEY));
            }

            if (headers.containsKey(USER_ID_KEY)) {
                builder.userId(headers.get(USER_ID_KEY));
            }
        }
        return builder.build();
    }

    private void enablePublisherConfirms() throws EventDeliveryException {
        try {
            rmqChannel.confirmSelect();
        } catch (IOException ex) {
            logger.error("Error enabling Publisher confirmations: {}", ex.toString());
            closeRabbitMQConnection();
            throw new EventDeliveryException(ex.toString());
        }
    }

    private Transaction getTransaction() {
        return getChannel().getTransaction();
    }

    private void maybeConnectToRabbitMQ() throws EventDeliveryException {
        if (connection == null) {
            connection = createRabbitMQConnection(factory);
            rmqChannel = createRabbitMQChannel();
            if (publisherConfirms) {
                enablePublisherConfirms();
            }
        }
    }

    private void publishMessage(Event event) throws EventDeliveryException {
        String rk;

        Map<String, String> headers = event.getHeaders();

        // Use a headers supplied routing key if it exists
        if (headers.containsKey(ROUTING_KEY)) {
            rk = headers.get(ROUTING_KEY);
        } else {
            rk = routingKey;
        }

        try {
            rmqChannel.basicPublish(exchange, rk, mandatory, createProperties(headers), event.getBody());
        } catch (IOException ex) {
            logger.error("Error publishing event message: {}", ex.toString());
            closeRabbitMQConnection();
            throw new EventDeliveryException(ex.toString());
        }

        if (publisherConfirms) waitForConfirmation();
    }

    private void waitForConfirmation() throws EventDeliveryException {
        try {
            rmqChannel.waitForConfirms();
        } catch (InterruptedException ex) {
            logger.error("Error waiting for publisher confirmation: {}", ex.toString());
            closeRabbitMQConnection();
            throw new EventDeliveryException(ex.toString());
        }
    }

}
