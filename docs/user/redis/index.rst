Redis Data Store
================

The GeoMesa Redis Data Store is an implementation of the GeoTools ``DataStore`` interface that is backed
by `Redis <https://redis.io/>`__. It is found in the ``geomesa-redis`` directory of the GeoMesa
source distribution.

.. pull-quote::

    Redis is an open source, in-memory data structure store, used as a database, cache and message broker.

Because Redis keeps everything in memory, it is extremely fast. However, it is generally not feasible to use for
very large data sets due to the hardware requirements. The Redis Data Store is a good choice for storing
the current state of a streaming set of features, or as part of a tiered storage architecture for high access
data, but not necessarily for long-term historical data.

To get started with the Redis Data Store, try the :doc:`/tutorials/geomesa-quickstart-redis` tutorial.

.. toctree::
    :maxdepth: 1

    install
    configuration
    index_config
    usage
    geoserver
    commandline
    visibilities
