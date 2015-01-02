rabbitmq-flume-plugin
=====================
A Flume plugin that provides a RabbitMQ *Source*, *Sink*, and *Interceptors*. While
there are other Flume(ng) plugins that do this as well, this implementation aims
to be highly performant and provide tools for mapping message properties to Flume
event headers.

This plugin was developed and tested against Flume 1.5.0.

Download
--------
No downloads available, this has yet to be released. The intention is to provide
a **jar** file for download in the releases page on GitHub.

Installation Instructions
-------------------------
To install the plugin, copy the *jar* file to the ``$FLUME_LIB`` directory. For
example, the ``$FLUME_LIB`` directory for Cloudera (CDH4) installed Flume, the
``$FLUME_LIB`` is ``/usr/lib/flume/lib``.

Configuration
-------------
Each component has its own configuration directives, but the **Source** and **Sink**
share common RabbitMQ connection parameters.

### Source
The RabbitMQ Source component enables RabbitMQ to act as a source for Flume events.
You must create and bind the queue you would like the source to consume from manually
in RabbitMQ, prior to starting Flume.

Variable          | Default       | Description
----------------- | ------------- | -----------
host              | ``localhost`` | The RabbitMQ host to connect to
port              | ``5672``      | The port to connect on
ssl               | ``false``     | Connect to RabbitMQ via SSL
exclude-protocols | ``SSLv3``     | A comma separated list of SSL protocols to exclude/ignore
virtual-host      | ``/``         | The virtual host name to connect to
username          | ``guest``     | The username to connect as
password          | ``guest``     | The password to use when connecting
queue             |               | **Required** field specifying the name of the queue to consume from
no_ack            | ``false``     | Toggle ``Basic.Consume`` no_ack mode
prefetch-count    | ``0``         | The ``Basic.QoS`` prefetch count to specify for consuming
prefetch-size     | ``0``         | The ``Basic.QoS`` prefetch size to specify for consuming
threads           | ``1``         | The number of consumer threads to create

#### Example

.. code::

    a1.sources.r1.channels = c1
    a1.sources.r1.type = com.aweber.flume.source.rabbitmq.RabbitMQSource
    a1.sources.r1.hostname = localhost
    a1.sources.r1.port = 5672
    a1.sources.r1.virtual_host = /
    a1.sources.r1.username = flume
    a1.sources.r1.password = rabbitmq
    a1.sources.r1.queue = events_for_s3
    a1.sources.r1.prefetch_count = 10

### Interceptors
TBD

### Sink
The RabbitMQ Sink allows for Flume events to be published to RabbitMQ. You must

Variable           | Default       | Description
------------------ | ------------- | -----------
host               | ``localhost`` | The RabbitMQ host to connect to
port               | ``5672``      | The port to connect on
virtual-host       | ``/``         | The virtual host name to connect to
username           | ``guest``     | The username to connect as
password           | ``guest``     | The password to use when connecting
exchange           | ``amq.topic`` | The exchange to publish the message to
routing_key        |               | The routing key to use when publishing
auto_properties    | ``false``     | Automatically populate AMQP message properties (timestamp, message_id, type?)
mandatory-publish  | ``false``     | Enable mandatory publishing
publisher-confirms | ``false``     | Enable publisher confirmations
transactional      | ``false``     | Enable transactional publishing (slowest publishing guarantee)
ssl                | ``false``     | Connect to RabbitMQ via SSL
exclude-protocols  | ``SSLv3``     | A comma separated list of SSL protocols to exclude/ignore
threads            | ``1``         | The number of consumer threads created.

#### Example

.. code::

    a1.sinks.k1.channels = c1
    a1.sinks.k1.type = com.aweber.flume.sink.rabbitmq.RabbitMQSink
    a1.sinks.k1.hostname = localhost
    a1.sinks.k1.port = 5672
    a1.sinks.k1.virtual_host = /
    a1.sinks.k1.username = flume
    a1.sinks.k1.password = rabbitmq
    a1.sinks.k1.exchange = amq.topic
    a1.sinks.k1.routing_key = flume.event
    a1.sinks.k1.publisher_confirms = true

Build Instructions
------------------
To build from source, use ``maven``:

.. code:: bash

    mvn package

This will download all of the dependencies required for building the plugin and
provide a **jar** file in the ``target/`` directory.
