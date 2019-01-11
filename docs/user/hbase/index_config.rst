HBase Index Configuration
=========================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
This section contains HBase-specific options; general options can be found under :ref:`index_config`.

Setting File Compression
------------------------

You can enable HBase file compression when creating a new ``SimpleFeatureType`` by setting the appropriate
user data hints, or through the command line option. Valid compression types
are ``snappy``, ``lzo``, ``gz``, ``bzip2``, ``lz4`` or ``zstd``.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("geomesa.table.compression.enabled", "true");
    sft.getUserData().put("geomesa.table.compression.type", "snappy");

.. code-block:: shell

    geomesa-hbase create-schema --compression snappy ...

For more information on how to set schema options, see :ref:`set_sft_options`.

.. _hbase_index_versions:

HBase Index Versions
====================

See :ref:`index_versioning` for an explanation of index versions. The following versions are available in HBase:

.. tabs::

    .. tab:: Z3

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.1.0           Initial implementation
        2             2.0.0           Uses fixed Z-curve implementation
        ============= =============== =================================================================

    .. tab:: Z2

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.3.0           Initial implementation
        2             2.0.0           Uses fixed Z-curve implementation
        ============= =============== =================================================================

    .. tab:: XZ3

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.3.0           Initial implementation
        ============= =============== =================================================================

    .. tab:: XZ2

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.3.0           Initial implementation
        ============= =============== =================================================================

    .. tab:: Attribute

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.3.0           Initial implementation
        2             1.3.1           Added secondary Z index
        3             1.3.2           Support for shards
        4             2.0.0-m.1       Internal row layout change
        5             2.0.0           Uses fixed Z-curve implementation
        ============= =============== =================================================================

    .. tab:: ID

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.3.0           Initial implementation
        ============= =============== =================================================================
