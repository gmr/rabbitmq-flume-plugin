package com.aweber.flume;

import com.aweber.flume.source.rabbitmq.RabbitMQSource;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurables;
import org.easymock.EasyMockRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

@RunWith(EasyMockRunner.class)
public class TestRabbitMQSource {

    private String queueName = "test-queue";
    private RabbitMQSource source;

    private Field getAccessibleField(String name) throws NoSuchFieldException {
        Field field = RabbitMQSource.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    @Before
    public void setUp() throws IOException {
        ConnectionFactory mock = createNiceMock(ConnectionFactory.class);
        expect(mock.newConnection()).andReturn(createNiceMock(Connection.class));
        replay(mock);
        source = new RabbitMQSource(mock);

        Context context = new Context();
        context.put("queue", queueName);
        Configurables.configure(source, context);
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
    public void testTimeoutDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals(-1, getAccessibleField("timeout").get(source));
    }

    @Test
    public void testAutoAckDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        assertEquals(false, getAccessibleField("autoAck").get(source));
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

