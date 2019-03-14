.. _accumulo_tools:

Accumulo Command-Line Tools
===========================

The GeoMesa Accumulo distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`setting_up_accumulo_commandline`.

Once installed, the tools should be available through the command ``geomesa-accumulo``::

    $ geomesa-accumulo
    INFO  Usage: geomesa-accumulo [command] [command options]
      Commands:
        ...

Commands that are common to multiple back ends are described in :doc:`/user/cli/index`. The commands
here are Accumulo-specific.

General Arguments
-----------------

Most commands require you to specify the connection to Accumulo. This generally includes a username and
password (or Kerberos keytab file). Specify the username and password with ``--user`` and ``--password``
(or ``-u`` and ``-p``). In order to avoid plaintext passwords in the bash history and process list,
the password argument may be omitted, in which case it will be prompted for instead.

To use Kerberos authentication instead of a password, use ``--keytab`` with a path to a Kerberos keytab
file containing an entry for the specified user. Since a keytab file allows authentication without any
further constraints, it should be protected appropriately.

If the necessary environment variables are set (generally as part of the install process), the tools should
connect automatically to the Accumulo instance. To specify the connection instead, use ``--instance-name``
and ``--zookeepers`` (or ``-i`` and ``-z``).

The ``--auths`` and ``--visibilities`` arguments correspond to the ``AccumuloDataStore`` parameters
``geomesa.security.auths`` and ``geomesa.security.visibilities``, respectively. See :ref:`authorizations`
and :ref:`accumulo_visibilities` for more information.

The ``--mock`` argument can be used to run against a mock Accumulo instance, for testing. In particular,
this can be useful for verifying ingest converters.

Commands
--------

.. _add_index_command:

``add-index``
^^^^^^^^^^^^^

Add or update indices for an existing feature type. This can be used to upgrade-in-place, converting an older
index format into the latest. See :ref:`index_upgrades` for more information.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``--index *``            The name of the index to add (z2, z3, etc)
``-q, --cql``            A filter to apply for back-filling data
``--no-back-fill``       Skip back-filling data
======================== =========================================================

The ``--index`` argument specifies the index to add. It must be the name of one of the known index types, e.g. ``z3``
or ``xz3``. See :ref:`index_overview` for available indices.

By default, the command will launch a map/reduce job to populate the new index with any existing features in the
schema. For large data sets, this may not be desired. The ``--no-back-fill`` argument can be used to disable index
population entirely, or ``--cql`` can be used to populate the index with a subset of the existing features.

When running this command, ensure that the appropriate authorizations and visibilities are set. Otherwise data
might not be back-filled correctly.

``add-attribute-index``
^^^^^^^^^^^^^^^^^^^^^^^

Add an index on an attribute. Attributes can be indexed individually during schema creation; this command can
add a new index in an existing schema. See :ref:`attribute_indices` for more information on indices.

This command is a convenience wrapper for launching the map/reduce job described in :ref:`attribute_indexing_job`.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-a, --attributes *``   Attribute(s) to index, comma-separated
``--coverage *``         Type of index, either ``join`` or ``full``
======================== =========================================================

For a description of index coverage, see :ref:`accumulo_attribute_indices`.

.. _compact_command:

``compact``
^^^^^^^^^^^

Incrementally compact tables for a given feature type.
`Compactions <https://accumulo.apache.org/1.9/accumulo_user_manual.html#_compaction>`__ in Accumulo will merge
multiple data files into a single file, which has the side effect of permanently deleting rows which have been
marked for deletion. Compactions can be triggered through the Accumulo shell; however queuing up too many
compactions at once can impact the performance of a cluster. This command will handle compacting all the tables
for a given feature type, and throttle the compactions so that only a few are running at one time.

======================== =============================================================
Argument                 Description
======================== =============================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``--threads``            Number of ranges to compact simultaneously, by default 4
``--from``               How long ago to compact data, based on the default date attribute, relative to current time.
                         E.g. '1 day', '2 weeks and 1 hour', etc
``--duration``           Amount of time to compact data, based on the default date attribute, relative to ``--from``.
                         E.g. '1 day', '2 weeks and 1 hour', etc
``--z3-feature-ids``     Indicates that feature IDs were written using the Z3FeatureIdGenerator. This allows
                         optimization of compactions on the ID table, based on the configured ``time``. See
                         :ref:`id_generator_config` for more information
======================== =============================================================

The ``--from`` and ``--duration`` parameters can be used to reduce the number of files that need to be compacted,
based on the default date attribute for the schema. Due to table keys, this is mainly useful for the Z3 index,
and the ID index when used with ``--z3-feature-ids``. Other indices will typically be compacted in full, as they
are not partitioned by date.

This command is particularly useful when using :ref:`ageoff_accumulo`, to ensure that expired rows are physically
deleted from disk. In this scenario, the ``--from`` parameter should be set to the age-off period, and the
``--duration`` parameter should be set based on how often compactions are run. The intent is to only compact
the data that may have aged-off since the last compaction. Note that the time periods align with attribute-based
age-off; ingest time age-off may need a time buffer, assuming some relationship between ingest time and the default
date attribute.

This command can also be used to speed up queries by removing entries that are duplicated or marked for deletion.
This may be useful for a static data set, which will not be automatically compacted by Accumulo once the size
stops growing. In this scenario, the ``--from`` and ``--duration`` parameters can be omitted, so that the
entire data set is compacted.

``configure-age-off``
^^^^^^^^^^^^^^^^^^^^^

List, add or remove age-off on a given feature type. See :ref:`ageoff_accumulo` for more information.

======================== =============================================================
Argument                 Description
======================== =============================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-l, --list``           List any age-off configured for the schema
``-r, --remove``         Remove age-off for the schema
``-s, --set``            Set age-off for the schema (requires ``--expiry``)
``-e, --expiry``         Duration before entries are aged-off('1 day', '2 weeks and 1 hour', etc)
``--dtg``                Use attribute-based age-off on the specified date field
======================== =============================================================

The ``--list`` argument will display any configured age-off.

The ``--remove`` argument will remove any configured age-off.

The ``--set`` argument will configure age-off. When using ``--set``, ``--expiry`` must also be provided.
``--expiry`` can be any time duration string, specified in natural language. If ``--dtg`` is provided,
age-off will be based on the specified date-type attribute. Otherwise, age-off will be based on ingest
time.

``configure-stats``
^^^^^^^^^^^^^^^^^^^

List, add or remove stat iterator configuration on a given catalog table. GeoMesa automatically configures an
iterator on the summary statistics table (``_stats``). Generally this does not need to be modified, however
if the Accumulo classpath is mis-configured, or data gets corrupted, it may be impossible to delete the
table without first removing the iterator configuration.

======================== =============================================================
Argument                 Description
======================== =============================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-l, --list``           List any stats iterator configured for the catalog table
``-r, --remove``         Remove the stats iterator configuration for the catalog table
``-a, --add``            Add the stats iterator configuration for the catalog table
======================== =============================================================

The ``--list`` argument will display any configured stats iterator.

The ``--remove`` argument will remove any configured stats iterator.

The ``--add`` argument will add the stats iterator.

``configure-table``
^^^^^^^^^^^^^^^^^^^

The command will list and update properties on the Accumulo tables used by GeoMesa. It has two
sub-commands:

* ``list`` List the configuration options for a table
* ``update`` Update a given configuration option for a table

To invoke the command, use the command name followed by the subcommand, then any arguments. For example::

    $ geomesa-accumulo configure-table list --catalog ...

======================== =============================================================
Argument                 Description
======================== =============================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``--index *``            The index table to examine/update (z2, z3, etc)
``-k, --key``            Property name to operate on (required for update sub-command)
``-v, --value *``        Property value to set (only for update sub-command)
======================== =============================================================

The ``--index`` argument specifies the index to examine. It must be the name of one of the known index types,
e.g. ``z3`` or ``xz3``. See :ref:`index_overview` for available indices. Note that not all
schemas will have all index types.

The ``--key`` argument can be used during both list and update. For list, it will filter the properties to
only show the one requested. For update, it is required as the property to update.

The ``--value`` argument is only used during update.

.. _accumulo_tools_stats_analyze:

``stats-analyze``
^^^^^^^^^^^^^^^^^

This command will re-generate the cached data statistics maintained by GeoMesa. This may be desirable for
several reasons:

* Stats are compiled incrementally during ingestion, which can sometimes lead to reduced accuracy
* Most stats are not updated when features are deleted, as they do not maintain enough information to handle deletes
* Errors or data corruption can lead to stats becoming unreadable

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
======================== =========================================================

.. _accumulo_tools_raster:

``ingest-raster``
^^^^^^^^^^^^^^^^^

.. warning::

  GeoMesa raster support is deprecated and will be removed in a future version.

Ingest one or more raster image files into Geomesa. Input files, GeoTIFF or DTED, should be located
on the local file system.

.. warning::

    In order to ingest rasters, ensure that you install JAI and JLine as described under
    :ref:`setting_up_accumulo_commandline`.

Input raster files are assumed to have CRS of ``EPSG:4326``. Non-``EPSG:4326`` files will need to be
converted into ``EPSG:4326`` raster files before ingestion. An example of doing conversion with GDAL is::

    gdalwarp -t_srs EPSG:4326 input_file out_file

======================== =========================================================
Argument                 Description
======================== =========================================================
``-t, --raster-table *`` Accumulo table for storing raster data
``-f, --file *``         A single raster file or a directly containing raster files to ingest
``-F, --format``         The format of raster files, which must match the file extension
``-P, --parallel-level`` Maximum number of local threads for ingesting multiple raster files
``-T, --timestamp``      Ingestion time (defaults to current time)
``--write-memory``       Memory allocation for ingestion operation
``--write-threads``      Numer of threads used for writing raster data
``--query-threads``      Number of threads used for querying raster data
======================== =========================================================

.. warning::

    When ingesting rasters from a directory, ensure that the ``--format`` argument matches the file extension of
    the files. Otherwise, no files will be ingested.

``delete-raster``
^^^^^^^^^^^^^^^^^

Delete ingested rasters.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-t, --raster-table *`` Accumulo table for storing raster data
``--force``              Delete without prompting for confirmation
======================== =========================================================
