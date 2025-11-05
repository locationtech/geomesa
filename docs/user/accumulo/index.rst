Accumulo Data Store
===================

.. note::

    GeoMesa currently supports Accumulo {{accumulo_supported_versions}}.

.. warning::

    Support for Accumulo 2.0.1 has been deprecated, and will be removed in a future version.

The GeoMesa Accumulo Data Store module is an implementation of the
GeoTools ``DataStore`` that is backed by `Apache Accumulo`_. GeoMesa
also contains many other modules designed to operate with Accumulo,
found in ``geomesa-accumulo`` in the source distribution.
This includes client code and distributed iterator code for the
Accumulo tablet servers.

.. _Apache Accumulo: https://accumulo.apache.org/

To get started with the Accumulo Data Store, try the :doc:`/tutorials/geomesa-quickstart-accumulo` tutorial.

.. toctree::
   :maxdepth: 1

   install
   usage
   geoserver
   commandline
   configuration
   index_config
   jobs
   atomic_writes
   kerberos
   examples