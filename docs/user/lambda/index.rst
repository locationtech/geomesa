Lambda Data Store
=================

The GeoMesa Lambda data store module is an implementation of the GeoTools ``DataStore`` that
keeps data in two tiers, a transient tier and a persistent tier. The transient storage is backed by `Apache Kafka`_.
Any other GeoMesa data store can be used for the persistent storage, although the current module only provides
configuration for `Apache Accumulo`_. The Lambda data store is found in ``geomesa-lambda`` in the source distribution.

.. _Apache Kafka: https://kafka.apache.org/
.. _Apache Accumulo: https://accumulo.apache.org/

To get started with the Lambda data store, try the :doc:`/tutorials/geomesa-quickstart-lambda` tutorial.

.. toctree::
   :maxdepth: 1

   overview
   install
   usage
   geoserver
   commandline
   configuration
   advanced
