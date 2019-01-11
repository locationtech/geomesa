Kudu Data Store
===============

.. note::

    GeoMesa currently supports Kudu version |kudu_version|.

The GeoMesa Kudu Data Store is an implementation of the GeoTools ``DataStore`` interface that is backed
by `Apache Kudu <http://kudu.apache.org/>`__. It is found in the ``geomesa-kudu`` directory of the GeoMesa
source distribution.

.. pull-quote::

    Apache Kudu completes Hadoop's storage layer to enable fast analytics on fast data.

GeoMesa leverages Kudu predicate push-down, column selection, and more, all through the GeoTools API. The Kudu
Data Store is a good choice for running Spark analytics on a few attributes at a time, as the columnar storage format
minimizes the underlying data reads. In addition, due to Kudu's compression options and ability to rapidly query
non-indexed columns, space on disk is minimized compared to more complex systems like HBase.

.. warning::

    The GeoMesa Kudu data store is an alpha-level feature, and hasn't been robustly tested at scale.

To get started with the Kudu Data Store, try the :doc:`/tutorials/geomesa-quickstart-kudu` tutorial.

.. toctree::
    :maxdepth: 1

    install
    configuration
    index_config
    usage
    geoserver
    commandline
    spark
    visibilities
