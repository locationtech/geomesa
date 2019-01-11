HBase Data Store
================

.. note::

    GeoMesa currently supports HBase version |hbase_version|.

The GeoMesa HBase Data Store is an implementation of the GeoTools
``DataStore`` interface that is backed by `Apache HBase`_.
It is found in the ``geomesa-hbase`` directory of the GeoMesa
source distribution.

.. _Apache HBase: https://hbase.apache.org/

To get started with the HBase Data Store, try the :doc:`/tutorials/geomesa-quickstart-hbase` tutorial or the
:doc:`/tutorials/geomesa-hbase-s3-on-aws` tutorial for using HBase backed by S3 instead of HDFS.

.. toctree::
    :maxdepth: 1

    install
    coprocessor_install
    configuration
    usage
    geoserver
    commandline
    index_config
    heatmaps
    visibilities
    kerberos

