rabbitmq-flume-plugin
=====================
A Flume plugin that provides a RabbitMQ *Source* and *Sink*. While
there are other Flume(ng) plugins that do this as well, this implementation aims
to be highly performant and provide tools for mapping message properties to Flume
event headers.

This plugin was developed and tested against Flume 1.5.0.

[![Build Status](https://travis-ci.org/gmr/rabbitmq-flume-plugin.svg?branch=master)](https://travis-ci.org/gmr/rabbitmq-flume-plugin)

Download
--------
No downloads available, this has yet to be released. The intention is to provide
a **jar** file for download in the releases page on GitHub.

Installation Instructions
-------------------------
To install the plugin, copy the *jar* file to the ``$FLUME_LIB`` directory. For
example, the ``$FLUME_LIB`` directory for Cloudera (CDH5) installed Flume, the
``$FLUME_LIB`` is ``/usr/lib/flume/lib``.

Behavior
--------

### Source
The RabbitMQ Source component enables RabbitMQ to act as a source for Flume events.
You must create and bind the queue you would like the source to consume from manually
in RabbitMQ, prior to starting Flume.

When the Source consumes a message, any message properties(*) that are set on the message
will be added to the Flume ``Event`` headers, including the exchange and routing key
for the message. (*) This excludes the headers property which is a free-form key/value table.
This is currently excluded due to the complexity of correctly parsing and dealing with the
different data types that could be returned as values in the headers table.

With Flume ``Event`` headers, you could use the ``type`` property or ``routing-key`` as
part of the file path or name when using the HDFS sink.

By default, there is a single consumer thread in the RabbitMQ source.

Configuration
-------------
Each component has its own configuration directives, but the **Source** and **Sink**
share common RabbitMQ connection parameters.

### Source

The Source component has the following configuration options:

Variable          | Default       | Description
----------------- | ------------- | -----------
host              | ``localhost`` | The RabbitMQ host to connect to
port              | ``5672``      | The port to connect on
ssl               | ``false``     | Connect to RabbitMQ via SSL
virtual-host      | ``/``         | The virtual host name to connect to
username          | ``guest``     | The username to connect as
password          | ``guest``     | The password to use when connecting
queue             |               | **Required** field specifying the name of the queue to consume from
auto-ack          | ``false``     | Enable auto-acknowledgement for higher throughput with the chance of message loss
prefetch-count    | ``0``         | The ``Basic.QoS`` prefetch count to specify for consuming
prefetch-size     | ``0``         | The ``Basic.QoS`` prefetch size to specify for consuming
threads           | ``1``         | The number of consumer threads to create

#### Example

```
a1.sources.r1.channels = c1
a1.sources.r1.type = com.aweber.flume.source.rabbitmq.RabbitMQSource
a1.sources.r1.hostname = localhost
a1.sources.r1.port = 5672
a1.sources.r1.virtual_host = /
a1.sources.r1.username = flume
a1.sources.r1.password = rabbitmq
a1.sources.r1.queue = events_for_s3
a1.sources.r1.prefetch_count = 10
```

### Interceptors
TBD

### Sink
The RabbitMQ Sink allows for Flume events to be published to RabbitMQ. The sink

Variable           | Default       | Description
------------------ | ------------- | -----------
host               | ``localhost`` | The RabbitMQ host to connect to
port               | ``5672``      | The port to connect on
ssl                | ``false``     | Connect to RabbitMQ via SSL
virtual-host       | ``/``         | The virtual host name to connect to
username           | ``guest``     | The username to connect as
password           | ``guest``     | The password to use when connecting
exchange           | ``amq.topic`` | The exchange to publish the message to
routing-key        |               | The routing key to use when publishing
auto-properties    | ``true``      | Automatically populate AMQP message properties
mandatory-publish  | ``false``     | Enable mandatory publishing
publisher-confirms | ``false``     | Enable publisher confirmations

#### Headers
When publishing an event message, the RabbitMQ Sink will first look to the event
headers for a ``routing-key`` entry. If it is set, it will use that value when
publishing the message. If it is not set, it will fall back to the configured
routing-key value which defaults to an empty string.

If the ``auto-properties`` configuration option is enabled (default), the event
headers will be examined for standard AMQP Basic.Properties entries (sans the
``headers`` AMQP table). If the property is set in the event headers, it will be set
in the message properties. Additionally, if the ``app-id`` value is not set in the
headers, it will default to ``RabbitMQSink``. If ``timestamp`` is not set in the
headers, it will default to the current system time.

##### Available property keys

- app-id
- content-encoding
- content-type
- correlation-id
- delivery-mode
- expires
- message-id
- priority
- reply-to
- timestamp
- type
- user-id

#### Example

```
a1.sinks.k1.channels = c1
a1.sinks.k1.type = com.aweber.flume.sink.rabbitmq.RabbitMQSink
a1.sinks.k1.hostname = localhost
a1.sinks.k1.port = 5672
a1.sinks.k1.virtual_host = /
a1.sinks.k1.username = flume
a1.sinks.k1.password = rabbitmq
a1.sinks.k1.exchange = amq.topic
a1.sinks.k1.routing-key = flume.event
a1.sinks.k1.publisher-confirms = true
```

Build Instructions
------------------
To build from source, use ``maven``:

```bash
mvn package
```

This will download all of the dependencies required for building the plugin and
provide a **jar** file in the ``target/`` directory.
