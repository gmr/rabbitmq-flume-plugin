package com.aweber.flume;

import java.lang.IllegalArgumentException;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.LinkedList;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;

import org.apache.flume.conf.Configurables;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Before;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aweber.flume.source.rabbitmq.RabbitMQSource;


public class TestRabbitMQSource {

    private static final Logger logger = LoggerFactory
            .getLogger(TestRabbitMQSource.class);

    private String queueName = "test-queue";
    private RabbitMQSource source;
    private Channel channel;
    private InetAddress localhost;

    @Before
    public void setUp() throws UnknownHostException {

        localhost = InetAddress.getByName("127.0.0.1");
        source = new RabbitMQSource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        List<Channel> channels = new LinkedList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));

        Context context = new Context();
        context.put("queue", queueName);
        Configurables.configure(source, context);
    }

    private Field getAccessibleField(String name) throws NoSuchFieldException {
        Field field = RabbitMQSource.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    @Test
    public void testHostnameDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals("localhost", getAccessibleField("hostname").get(source));
    }

    @Test
    public void testPortDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals(5672, getAccessibleField("port").get(source));
    }

    @Test
    public void testSSLDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals(false, getAccessibleField("enableSSL").get(source));
    }

    @Test
    public void testExcludeProtocolsDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        List<String> expectation = new LinkedList<String>();
        expectation.add(0, "SSLv3");
        assertEquals(expectation, getAccessibleField("excludeProtocols").get(source));
    }

    @Test
    public void testExcludeProtocolsWithOneValue() throws NoSuchFieldException, IllegalAccessException {
        List<String> expectation = new LinkedList<String>();
        expectation.add(0, "TLSv1");
        expectation.add(1, "SSLv3");

        Context context = new Context();
        context.put("queue", queueName);
        context.put("exclude-protocols", "TLSv1");
        Configurables.configure(source, context);

        assertEquals(expectation, getAccessibleField("excludeProtocols").get(source));
    }

    @Test
    public void testExcludeProtocolsWithMultipleValues() throws NoSuchFieldException, IllegalAccessException {
        List<String> expectation = new LinkedList<String>();
        expectation.add(0, "TLSv1");
        expectation.add(1, "SSLv3");

        Context context = new Context();
        context.put("queue", queueName);
        context.put("exclude-protocols", "TLSv1 SSLv3");
        Configurables.configure(source, context);

        assertEquals(expectation, getAccessibleField("excludeProtocols").get(source));
    }

    @Test
    public void testVirtualHostDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals("/", getAccessibleField("virtualHost").get(source));
    }

    @Test
    public void testUsernameDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals("guest", getAccessibleField("username").get(source));
    }

    @Test
    public void testPasswordDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals("guest", getAccessibleField("password").get(source));
    }

    @Test
    public void testPrefetchCountDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals(0, getAccessibleField("prefetchCount").get(source));
    }

    @Test
    public void testPrefetchSizeDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals(0, getAccessibleField("prefetchSize").get(source));
    }

    @Test
    public void testNoAckDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals(false, getAccessibleField("noAck").get(source));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyQueue() {
        Context context = new Context();
        Configurables.configure(source, context);
    }

    @Test
    public void testQueuePassedValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals(queueName, getAccessibleField("queue").get(source));
    }

}

