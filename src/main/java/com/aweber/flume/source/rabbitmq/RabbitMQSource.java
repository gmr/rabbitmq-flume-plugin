package com.aweber.flume.source.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;


public class RabbitMQSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSource.class);
    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";
    private static final String SSL_KEY = "ssl";
    private static final String VHOST_KEY = "virtual-host";
    private static final String USER_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String QUEUE_KEY = "queue";
    private static final String AUTOACK_KEY = "auto-ack";
    private static final String PREFETCH_COUNT_KEY = "prefetch-count";
    private static final String TIMEOUT_KEY = "timeout";
    private static final String THREAD_COUNT_KEY = "threads";
    private SourceCounter sourceCounter;
    private ConnectionFactory factory;
    private CounterGroup counterGroup;
    private String hostname;
    private int port;
    private boolean enableSSL = false;
    private String virtualHost;
    private String username;
    private String password;
    private String queue;
    private boolean autoAck = false;
    private int prefetchCount = 0;
    private int timeout = -1;
    private int consumerThreads = 1;

    private List<Consumer> consumers;
    private List<Thread> threads;

    public RabbitMQSource() {
        this(new ConnectionFactory());
    }

    public RabbitMQSource(ConnectionFactory factory) {
        consumers = new LinkedList<Consumer>();
        threads = new LinkedList<Thread>();
        this.factory = factory;
    }

    @Override
    public void configure(Context context) {
        // Only the queue name does not have a default value
        Configurables.ensureRequiredNonNull(context, QUEUE_KEY);

        // Assign all of the configured values
        hostname = context.getString(HOST_KEY, ConnectionFactory.DEFAULT_HOST);
        port = context.getInteger(PORT_KEY, ConnectionFactory.DEFAULT_AMQP_PORT);
        enableSSL = context.getBoolean(SSL_KEY, false);
        virtualHost = context.getString(VHOST_KEY, ConnectionFactory.DEFAULT_VHOST);
        username = context.getString(USER_KEY, ConnectionFactory.DEFAULT_USER);
        password = context.getString(PASSWORD_KEY, ConnectionFactory.DEFAULT_PASS);
        queue = context.getString(QUEUE_KEY, null);
        autoAck = context.getBoolean(AUTOACK_KEY, false);
        prefetchCount = context.getInteger(PREFETCH_COUNT_KEY, 0);
        timeout = context.getInteger(TIMEOUT_KEY, -1);
        consumerThreads = context.getInteger(THREAD_COUNT_KEY, 1);

        // Ensure that Flume can connect to RabbitMQ
        testRabbitMQConnection();

        // Create and configure the counters
        sourceCounter = new SourceCounter(getName());
        counterGroup = new CounterGroup();
        counterGroup.setName(getName());
    }

    @Override
    public synchronized void start() {
        logger.info("Starting {} with {} thread(s)", getName(), consumerThreads);
        sourceCounter.start();
        for (int i = 0; i < consumerThreads; i++) {
            Consumer consumer = new Consumer()
                    .setHostname(hostname)
                    .setPort(port)
                    .setSSLEnabled(enableSSL)
                    .setVirtualHost(virtualHost)
                    .setUsername(username)
                    .setPassword(password)
                    .setQueue(queue)
                    .setPrefetchCount(prefetchCount)
                    .setTimeout(timeout)
                    .setAutoAck(autoAck)
                    .setChannelProcessor(getChannelProcessor())
                    .setSourceCounter(sourceCounter)
                    .setCounterGroup(counterGroup);
            Thread thread = new Thread(consumer);
            thread.setName("RabbitMQ Consumer #" + String.valueOf(i));
            thread.start();
            consumers.add(consumer);
            threads.add(thread);
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping {}", getName());
        for (int i = 0; i < consumerThreads; i++) {
        logger.debug("Stopping consumer #{}", i);
            Consumer consumer = consumers.get(i);
            consumer.shutdown();
        }
        sourceCounter.stop();
        super.stop();
    }

    private void testRabbitMQConnection() {
        Connection connection;

        factory.setHost(hostname);
        factory.setPort(port);
        factory.setVirtualHost(virtualHost);
        factory.setUsername(username);
        factory.setPassword(password);
        if (enableSSL) {
            try {
                factory.useSslProtocol();
            } catch (NoSuchAlgorithmException ex) {
                throw new IllegalArgumentException("Could not Enable SSL: " + ex.toString());
            } catch (KeyManagementException ex) {
                throw new IllegalArgumentException("Could not Enable SSL: " + ex.toString());
            }
        }
        try {
            connection = factory.newConnection();
            connection.close();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Could not connect to RabbitMQ: " + ex.toString());
        }
    }

}
