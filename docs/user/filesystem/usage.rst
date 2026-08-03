.. _fsds_parameters:

FileSystem Data Store Parameters
================================

Use the following parameters for a FileSystem data store:

=================================== ====== ========================================================================================
Parameter                           Type   Description
=================================== ====== ========================================================================================
``fs.catalog.type``                 String A convenience method for specifying the Iceberg catalog ``type``. See
                                           :ref:`fsds_metadata` for details
``fs.config.properties``            String Storage configuration properties, in Java properties format. See
                                           :ref:`fsds_config_props` and :ref:`fsds_metadata` for available properties. Environment
                                           variables in property values will be interpolated using ``${...}`` syntax
``fs.config.file``                  String The name of a file containing storage configuration properties, in Java properties
                                           format. See :ref:`fsds_config_props` and :ref:`fsds_metadata` for available properties.
                                           Environment variables in property values will be interpolated using ``${...}`` syntax
``fs.writer.partitions.max.open``   Int    When writing to multiple partitions at once, this restricts the maximum number of
                                           partition files to hold open at one time, per writer, defaults to ``32``
``geomesa.query.threads``           Int    The number of threads used for each query, defaults to ``4``
``geomesa.query.timeout``           String The max time a query will be allowed to run before being killed. The
                                           timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.security.auths``          String Comma-delimited superset of authorizations that will be used for queries. See
                                           :ref:`reading_vis_labels` for details
``geomesa.security.auth-provider``  String Class name for an ``AuthorizationsProvider`` implementation
``geomesa.metrics.registry``        String Specify the type of registry used to publish metrics. Must be one of ``none``,
                                           ``prometheus``, or ``cloudwatch``. See :ref:`geomesa_metrics` for registry details.
``geomesa.metrics.registry.config`` String Override the default registry config. See :ref:`geomesa_metrics` for configuration
                                           details.
=================================== ====== ========================================================================================

There are different ways to specify the configuration properties, but note that at least one of ``fs.catalog.type``,
``fs.config.properties`` or ``fs.config.file`` must be specified in order to load the FileSystem data store.

Programmatic Access
-------------------

An instance of a FileSystem data store can be obtained through the normal GeoTools discovery methods, assuming that
the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = Map.of(
      "fs.catalog.type", "rest",
      "fs.config.properties", String.join("\n", "uri=http://localhost:8080/", "iceberg.namespace=geomesa")
    );
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <https://docs.geotools.org/stable/userguide/>`_.
