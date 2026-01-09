.. _no_zookeeper:

Migration from Zookeeper
========================

Historically, the Kafka data store persisted schema information in Zookeeper. However, since Kafka has deprecated (in 3.x) and
then removed (in 4.x) support for Zookeeper, GeoMesa now stores schema information in Kafka itself.

To transition from a Zookeeper-based install to a purely Kafka one, re-create any existing feature types using the same topic.
The GeoMesa CLI provides a convenient method to do this:

.. code-block:: bash

    $ ./geomesa-kafka migrate-zookeeper-metadata -b localhost:9092 -z localhost:2181
    INFO  Migrating schema 'example-csv'
    INFO  Complete

See :ref:`kafka_tools` for details.
