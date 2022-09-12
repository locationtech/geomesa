.. _no_zookeeper:

Zookeeper-less Usage
====================

In anticipation of Kafka supporting Zookeeper-less installs, the Kafka Data Store no longer requires Zookeeper.
When getting a data store instance, simply omit the Zookeeper connection info to run in Zookeeper-free mode.

.. warning::

    It may appear that schemas have been lost if they are stored in Zookeeper and the Zookeeper connection
    is not specified.

To transition from a Zookeeper-based install to a Zookeeper-free one, re-create any existing feature types
using the same topic. This may be done easily through the GeoMesa CLI:

.. code-block:: bash

    $ ./geomesa-kafka migrate-zookeeper-metadata -b localhost:9092 -z localhost:2181
    INFO  Migrating schema 'example-csv'
    INFO  Complete

See :ref:`kafka_tools` for details.
