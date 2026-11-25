.. _pg_partition_parameters:

Partitioned PostGIS Data Store Parameters
=========================================

The partitioned PostGIS data store uses the same parameters as the standard PostGIS data store, except
that ``dbtype`` **must** be set to ``postgis-partitioned``. See the
`JDBCDataStore <https://docs.geotools.org/stable/userguide/library/jdbc/datastore.html>`__ and
`PostGIS Plugin <https://docs.geotools.org/stable/userguide/library/jdbc/postgis.html>`__ for other parameters.

The following additional parameters are also supported:

======================================= ======== ===================================================================================================================================
Parameter                               Type     Description
======================================= ======== ===================================================================================================================================
``idle_in_transaction_session_timeout`` Duration Transaction idle timeout (e.g. ``2 minutes``). See the
                                                 `Postgres documentation <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT>`__
                                                 for more information. Setting this timeout may help prevent abandoned queries from slowing down database operations.
``read_access_roles``                   String   A comma-separated list of roles that should be granted read-only access to any new schemas. These roles must already exist in the
                                                 database.
``prepare_threshold``                   Integer  The pgjdbc ``prepareThreshold`` connection property. By default (``5``), the JDBC driver only requests the binary result wire
                                                 format after a statement has been server-prepared, so one-shot queries are always decoded as the slower text format. Set to
                                                 ``-1`` to force the binary format on the first execution. Leave unset to use the pgjdbc default. See the
                                                 `pgjdbc documentation <https://jdbc.postgresql.org/documentation/use/>`__ for more information.
``geomesa.security.auths``              String   Comma-delimited superset of authorizations that will be used for queries. See :ref:`reading_vis_labels` for details
``geomesa.security.auth-provider``      String   Class name for an ``AuthorizationsProvider`` implementation
``geomesa.metrics.registry``            String   Specify the type of registry used to publish metrics. Must be one of ``none``,
                                                 ``prometheus``, or ``cloudwatch``. See :ref:`geomesa_metrics` for registry details.
``geomesa.metrics.registry.config``     String   Override the default registry config. See :ref:`geomesa_metrics` for configuration details.
======================================= ======== ===================================================================================================================================

Programmatic Access
-------------------

An instance of a PostGIS data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("dbtype", "postgis-partitioned");
    parameters.put("host", "localhost");
    parameters.put("port", "5432");
    parameters.put("database", "geomesa");
    parameters.put("user", "postgres");
    parameters.put("passwd", "postgres");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <https://docs.geotools.org/stable/userguide/>`_.

.. _postgis_update_schema:

Updating Existing Schemas
-------------------------

An existing feature type can be modified through the GeoTools ``updateSchema`` method. The following changes are
supported:

* Adding new attributes at the end of the feature type. New attributes must be nillable, and will be ``null`` for
  any existing features.
* Removing existing attributes, other than the default date and default geometry.
* Adding indices to existing attributes, by flagging the attributes for indexing in the updated feature type.
* Renaming the feature type, by passing in a feature type with a different name.
* Updating configuration that is not fixed when the schema is created - see :ref:`postgis_index_config` for details
  on which options can be changed. Any configuration that is not specified in the updated feature type will
  be left unchanged.

Re-ordering or renaming attributes, changing attribute types, and changing the default date or default geometry
are not supported.

Adding or Removing Attributes or Indices
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Adding or removing attributes, or adding an index to an existing attribute, creates a new *generation* of the
feature type. A new set of tables, procedures and scheduled jobs is created, along with a new main view. The new
view also exposes the data from the previous generation, with ``null`` values for any attributes that were added.
New writes go to the new tables.

Creating a new generation does not modify or lock the existing view or tables. This means that an update will not
block, or be blocked by, any long-running queries or partition maintenance.

The previous generation continues to run its own partition maintenance, including any configured age-off. As soon
as the previous generation does not contain any features, it is dropped automatically by the partition maintenance
for the current generation. Normally this happens once all of its data has aged off, but it will also happen if all
of its features are deleted, or if the feature type was empty when it was updated. Dropping the previous generation
requires a brief exclusive lock on the current main view; if the lock can't be acquired within one second, the
attempt is skipped and retried on the next maintenance run. If a feature type is not configured with age-off
(see :ref:`postgis_index_config`), then a previous generation that contains features will never be dropped, unless
the features are deleted. Each generation that has not been dropped adds some overhead to queries.

The new tables are created with the indices specified in the updated feature type. The tables for the previous
generation are not modified, so an index that is added to an existing attribute will only be used for features that
are written after the update. To index existing features, indices can be added through ``CREATE INDEX`` statements,
as described in :ref:`postgis_index_config`.

The data for a removed attribute is not deleted until the generation containing it is dropped, but it is no longer
accessible through the data store. Changes to the age-off configuration are applied to all generations, but any
other configuration changes will only apply to the current generation.

.. warning::

    Feature IDs are only checked for uniqueness within a generation. Normally, a feature that is written with the
    same feature ID and date as an existing feature is ignored. However, if the existing feature is in a previous
    generation, then the feature will be written to the current generation, and queries will return both copies
    until the previous generation is dropped. A subsequent update or delete will only modify the copy in the
    current generation. Avoid re-ingesting data that was written before a schema was updated, unless the
    existing features are deleted first.

.. note::

    Upgrading a schema (see :ref:`postgis_partition_upgrade`) only re-creates the procedures and triggers for the
    current generation. A previous generation keeps the procedures, triggers and scheduled jobs that it was created
    with, from the version of GeoMesa that created it, until it is dropped. This means that fixes or improvements
    to partition maintenance in a newer version of GeoMesa will not apply to the data in a previous generation.

The new view and tables are named based on the feature type name with a unique suffix, for example
``my_type_0001``. As such, direct SQL access should not rely on the name of the view. A view can be associated
with a type name through the ``geomesa_userdata`` table under the key ``pg.type.name``. Any privileges that were
granted on the previous view will be granted on the new view.

.. note::

    Privileges that were granted directly on the underlying tables of the previous generation are not carried over
    to the new tables, other than read access for any roles configured through ``read_access_roles``. Writes through
    the view are applied to the underlying tables as the current role, so a role that does not own the tables needs
    privileges on them in order to write. As new tables are also created during regular partition maintenance,
    privileges for such roles should be configured as default privileges on the schema, instead of being granted
    on individual tables. For example:

    .. code-block:: sql

        ALTER DEFAULT PRIVILEGES FOR ROLE my_owner IN SCHEMA my_schema
          GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO my_writer;

Other data store instances that have already loaded the feature type, will continue to use the previous view
until their cached copy of the feature type expires. The expiration is controlled by the system property
``geomesa.metadata.expiry``, which defaults to ``10 minutes``. Long-lived feature writers must be re-created
in order to pick up the changes. Anything that accesses the view directly through SQL should be switched to the
new view.

Any database objects that depend on the previous view, such as user-created views, will prevent the previous
generation from being dropped. In this case, partition maintenance will log a warning each time it runs, and will
retry until the dependent objects have been dropped or re-created against the new view.

Renaming Feature Types
^^^^^^^^^^^^^^^^^^^^^^

Renaming a feature type only changes the name that is exposed through the data store. The main view and the
underlying tables, procedures and scheduled jobs will keep their existing names, and no locks are required. The
mapping of the feature type name to the view is stored in the ``geomesa_userdata`` table under the key
``pg.type.name``.

Updating Configuration
^^^^^^^^^^^^^^^^^^^^^^

Updating configuration will re-write the partition procedures in place, the same as running the
:ref:`postgis_partition_upgrade` command. The main view is only replaced if its definition has changed (for example,
after upgrading GeoMesa versions), so normally this will not block, or be blocked by, any long-running queries or
partition maintenance. If the view does need to be replaced, it requires an exclusive lock on the view, which will
wait for any in-flight queries to complete, and queue any subsequent queries behind it.
