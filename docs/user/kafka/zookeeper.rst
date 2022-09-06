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

    $ ./geomesa-kafka get-sft-config -f example-csv -b localhost:9092 -z localhost:2181 > sft-definition
    INFO  Retrieving SFT for type name 'example-csv'
    $ cat sft-definition
    fid:Integer,name:String:index=true,lastseen:Date,*geom:Point:srid=4326;geomesa.kafka.topic='geomesa-catalog-example-csv',geomesa.index.dtg='lastseen',geomesa.kafka.partitioning='default'
    # note: the following command omits the -z flag for zookeeper
    $ ./geomesa-kafka create-schema -f example-csv -s $(cat sft-definition) -b localhost:9092
    INFO  Creating 'example-csv' with spec 'fid:Integer,name:String,age:Integer,lastseen:Date,*geom:Point:srid=4326'. Just a few moments...
    INFO  Created schema 'example-csv'
