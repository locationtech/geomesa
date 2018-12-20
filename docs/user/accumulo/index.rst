Accumulo Data Store
===================

.. note::

    GeoMesa currently supports Accumulo version |accumulo_version|.

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
   configuration
   usage
   geoserver
   commandline
   examples
   visibilities
   data_management
   jobs
   raster
   internals
   kerberos
   ageoff