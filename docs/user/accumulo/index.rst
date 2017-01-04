Accumulo Data Store
===================

The GeoMesa Accumulo Data Store module is an implementation of the
GeoTools ``DataStore`` that is backed by `Apache Accumulo`_. GeoMesa
also contains many other modules designed to operate with Accumulo,
found in ``geomesa-accumulo`` in the source distribution.
This includes client code and distributed iterator code for the
Accumulo tablet servers.

.. _Apache Accumulo: https://accumulo.apache.org/

For installation instructions, see :doc:`/user/installation/accumulo`.

To get started with the Accumulo Data Store, try the :doc:`/tutorials/geomesa-quickstart-accumulo` tutorial.

.. toctree::
   :maxdepth: 2

   configuration
   commandline_tools
   examples
   usage
   geoserver
   visibilities
   authorizations
   data_management
   explaining
   analytic_queries
   jobs
   raster
   internals