package com.aweber.flume;

import java.lang.IllegalArgumentException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;

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

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aweber.flume.source.rabbitmq.RabbitMQSource;


public class TestRabbitMQSource {

    private static final Logger logger = LoggerFactory
            .getLogger(TestRabbitMQSource.class);

    private RabbitMQSource source;
    private Channel channel;
    private InetAddress localhost;

    @Before
    public void setUp() throws UnknownHostException {

        localhost = InetAddress.getByName("127.0.0.1");
        source = new RabbitMQSource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testEmptyQueue() {
        Context context = new Context();
        Configurables.configure(source, context);
    }

}
