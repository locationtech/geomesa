.. _trino_index_page:

Trino Data Store
================

The Trino data store brings support for spatial queries to `Apache Iceberg <https://iceberg.apache.org/>`_ tables stored as
Parquet on S3-compatible object storage.

.. note::

    GeoMesa currently supports Trino {{trino_supported_versions}}.

.. note::

    The module is currently scoped as a **read-only** query engine — tables are created
    and populated by external writers, in particular the :doc:`FileSystem data store </user/filesystem/index>`.

.. toctree::
    :maxdepth: 1

    design
    install
    usage
    configuration
    commandline
    security
