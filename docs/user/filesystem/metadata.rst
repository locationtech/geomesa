.. _fsds_metadata:

FileSystem Metadata
===================

The FileSystem data store (FSDS) stores metadata about partitions and data files, to avoid having to repeatedly
interrogate the filesystem. When a new data file is added or removed, an associated metadata entry will be created
to track the operation. GeoMesa uses `Apache Iceberg <https://iceberg.apache.org/>`__ for storing metadata.

Iceberg-specific configuration properties are configured through the :ref:`fsds_parameters` ``fs.config.properties`` and
``fs.config.file``.

========================================= ===================================================================================
Key                                       Description
========================================= ===================================================================================
``iceberg.namespace *``                   The namespace used for storing tables in Iceberg. This corresponds to different
                                          things, depending on the catalog used. For example, in Glue it's the "database".
``type``                                  The Iceberg catalog type to use. Currently supports ``hadoop``, ``hive``, ``rest``,
                                          ``glue``, ``nessie``, ``jdbc``, and ``bigquery``
``catalog-impl``                          Instead of ``type``, a fully-qualified catalog class name may be specified
========================================= ===================================================================================

Additional properties will depend on the catalog being used. See the
`Apache Iceberg documentation <https://iceberg.apache.org/docs/latest/catalog-properties/>`__ for full details on the available
properties.
