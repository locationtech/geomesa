.. _fsds_parameters:

FileSystem Data Store Parameters
================================

Use the following parameters for a FileSystem data store (required parameters are marked with ``*``):

================================== ====== ========================================================================================
Parameter                          Type   Description
================================== ====== ========================================================================================
``fs.path *``                      String The root path to write and read data from (e.g. ``s3a://mybucket/datastores/testds``)
``fs.metadata.type``               String Format for storing metadata. Must be one of ``file``, ``jdbc`` or ``converter``. May
                                          alternatively be specified in the ``fs.config.properties`` or ``fs.config.file``
                                          parameter. See :ref:`fsds_metadata` for details
``fs.config.properties``           String Storage configuration properties, in Java properties format. See
                                          :ref:`fsds_config_props` and :ref:`fsds_metadata` for available properties. Environment
                                          variables in property values will be interpolated using ``${...}`` syntax
``fs.config.file``                 String The name of a file containing storage configuration properties, in Java properties
                                          format. See :ref:`fsds_config_props` and :ref:`fsds_metadata` for available properties.
                                          Environment variables in property values will be interpolated using ``${...}`` syntax
``fs.encoding``                    String The file format to use, defaults to ``parquet``. Provided implementations are
                                          ``parquet`` and ``converter``
``fs.writer.partition.timeout``    String Timeout for closing a partition file after write, e.g. '60 seconds'. This is to
                                          prevent too many open files during large write operations
``geomesa.query.threads``          Int    The number of threads used for each query, defaults to ``4``
``geomesa.query.timeout``          String The max time a query will be allowed to run before being killed. The
                                          timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.security.auths``         String  Comma-delimited superset of authorizations that will be used for queries. See
                                           :ref:`reading_vis_labels` for details
``geomesa.security.auth-provider`` String  Class name for an ``AuthorizationsProvider`` implementation
================================== ====== ========================================================================================

Programmatic Access
-------------------

An instance of a FileSystem data store can be obtained through the normal GeoTools discovery methods, assuming that
the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = Map.of(
      "fs.path", "hdfs://localhost:9000/fs-root/",
      "fs.metadata.type", "file"
    );
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <https://docs.geotools.org/stable/userguide/>`_.
