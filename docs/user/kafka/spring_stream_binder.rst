Spring Cloud Stream Geomesa Kafka Datastore Binder
==================================================

The Spring Cloud Stream Geomesa Kafka Datastore Binder provides an easy way for Spring Cloud Stream apps to hook into
Geomesa Kafka Datastore to process events.

If you are unfamiliar with Spring Cloud Stream, see the official documentation for an introduction:
https://spring.io/projects/spring-cloud-stream

Input/Output Types
------------

This binder will provide all ``KafkaFeatureEvent`` s from kafka datastore to your configured function definitions. Each
function will have to do it's own type comparison to see if the event is a ``KafkaFeatureEvent.KafkaFeatureChanged``,
``KafkaFeatureEvent.KafkaFeatureRemoved``, or another event type.

The module also ships with a SimpleFeature converter, which allows you to configure function definitions that consume
or produces ``SimpleFeature`` s and avoid working with ``KafkaFeatureEvent`` s directly.

.. note::

    The SimpleFeature converter extracts the SimpleFeature out of ``KafkaFeatureEvent.KafkaFeatureChanged`` events and
    ignores all others. Any function definition that consumes SimpleFeatures will miss the
    ``KafkaFeatureEvent.KafkaFeatureRemoved`` and ``KafkaFeatureEvent.KafkaFeatureCleared`` messages. And any function
    definition that only writes SimpleFeatures will not be able to send those messages.

Configuration
-------------

The configuration options are under spring.cloud.stream.kafka-datastore.binder. This binder will accept any
configuration options for the standard java geomesa kafka-datastore, with the periods ('.') replaced with dashes ('-').
For example, to specify kafka.catalog.topic for the binder, set:

.. code-block:: yaml

    spring:
      cloud:
        stream:
          kafka-datastore:
            binder:
              kafka-catalog-topic: geomesa-catalog-topic

For a full list of configuration options, see: https://www.geomesa.org/documentation/stable/user/kafka/usage.html

Examples
--------

Simple Logger App
-----------------

.. code-block:: java

    @Bean
    public Consumer<KafkaFeatureEvent> log() {
        return obj -> logger.info(obj.toString());
    }

.. code-block:: yaml

    spring:
      cloud:
        function:
          definition: log
        stream:
          kafka-datastore.binder:
                kafka-brokers: kafka:9092
                kafka-zookeepers: zookeeper:2181
          function.bindings:
            log-in-0: input
          bindings:
            input:
              destination: messages
              group: logger

Simple Enricher App
-------------------

.. code-block:: java

    @Bean
    public Function<SimpleFeature, SimpleFeature> attachSourceField() {
        return sf -> {
            sf.setAttribute("source", "un-labelled source");
            return sf;
        };
    }

.. code-block:: yaml

    spring:
      cloud:
        function:
          definition: attachSourceField
        stream:
          kafka-datastore.binder:
              kafka-brokers: kafka:9092
              kafka-zookeepers: zookeeper:2181
          function.bindings:
            attachSourceField-in-0: input
            attachSourceField-out-0: output
          bindings:
            input:
              destination: un-labelled-source-ob
              group: sft-reader
            output:
              destination: observations
              group: sft-writer

Simple Filter App
-------------------

.. code-block:: java

    @Bean
    public Function<SimpleFeature, SimpleFeature> excludeMoving() {
        return sf -> {
            if (sf.getAttribute("status").equals("IN_TRANSIT")) {
                return null;
            }
            return sf;
        };
    }


.. code-block:: yaml

    spring:
      cloud:
        function:
          definition: filterMoving
        stream:
          kafka-datastore.binder:
              kafka-brokers: kafka:9092
              kafka-zookeepers: zookeeper:2181
          function.bindings:
            filterMoving-in-0: input
            filterMoving-out-0: output
          bindings:
            input:
              destination: movingAndUnmovingThings
              group: sft-reader
            output:
              destination: unMovingThings
              group: sft-writer

Multiple Datastore App
----------------------

In the case of multi-bindings, you simply need to submit override the proper kafka-datastore fields in the environment
field.

.. code-block:: java

    @Bean
    public Function<KafkaFeatureEvent, KafkaFeatureEvent> passThrough() {
        return event -> event;
    }

.. code-block:: yaml

    spring:
      cloud:
        function:
          definition: passThrough
        stream:
          kafka-datastore.binder:
              kafka-brokers: kafka:9092
              kafka-zookeepers: zookeeper:2181
          function.bindings:
            passThrough-in-0: input
            passThrough-out-0: output
          binders:
            kds-start:
              type: kafka-datastore
              environment:
                spring.cloud.stream.kafka-datastore.binder:
                    kafka-zk-path: geomesa/start
            kds-end:
              type: kafka-datastore
              environment:
                spring.cloud.stream.kafka-datastore.binder:
                    kafka-zk-path: geomesa/end
          bindings:
            input:
              destination: observations
              group: sft-reader
              binder: kds-start
            output:
              destination: observations
              group: sft-writer
              binder: kds-end
