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

Most commands require you to specify the connection to Accumulo. This generally includes the instance name,
zookeeper hosts, username, and password (or Kerberos keytab file). Specify the instance with ``--instance-name``
and ``--zookeepers``, and the username and password with ``--user`` and ``--password``. The password argument may be
omitted in order to avoid plaintext credentials in the bash history and process list - in this case it will be
prompted case for later. To use Kerberos authentication instead of a password, use ``--keytab`` with a path to a
Kerberos keytab file containing an entry for the specified user. Since a keytab file allows authentication
without any further constraints, it should be protected appropriately.

Instead of specifying the cluster connection explicitly, an appropriate ``accumulo-client.properties``
may be added to the classpath. See the
`Accumulo documentation <https://accumulo.apache.org/docs/2.x/getting-started/clients#creating-an-accumulo-client>`_
for information on the necessary configuration keys. Any explicit command-line arguments will take precedence over
the configuration file.

The ``--auths`` argument corresponds to the ``AccumuloDataStore`` parameter ``geomesa.security.auths``. See
:ref:`data_security` for more information.

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

``bulk-ingest``
^^^^^^^^^^^^^^^

The bulk ingest command will ingest directly to Accumulo RFiles and then import the RFiles into Accumulo, bypassing
the normal write path. See `Bulk Ingest <https://accumulo.apache.org/docs/2.x/development/high_speed_ingest#bulk-ingest>`__
in the Accumulo documentation for additional details.

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
.. note::

  Bulk ingest is currently only implemented for Accumulo 2.0.

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
The data to be ingested must be in the same distributed file system that Accumulo is using, and the ingest
must run in ``distributed`` mode as a map/reduce job.

In order to run efficiently, you should ensure that the data tables have appropriate splits, based on
your input. This will avoid creating extremely large files during the ingest, and will also prevent the cluster
from having to subsequently split the RFiles. See :ref:`table_split_config` for more information.

Note that some of the below options are inherited from the regular ``ingest`` command, but are not relevant
to bulk ingest. See :ref:`cli_ingest` for additional details on the available options.

========================== ==================================================================================================
Argument                   Description
========================== ==================================================================================================
``-c, --catalog *``        The catalog table containing schema metadata
``--output *``             The output directory used to write out RFiles
``-f, --feature-name``     The name of the schema
``-s, --spec``             The ``SimpleFeatureType`` specification to create
``-C, --converter``        The GeoMesa converter used to create ``SimpleFeature``\ s
``--converter-error-mode`` Override the error mode defined by the converter
``-q, --cql``              If using a partitioned store, a filter that covers the ingest data
``-t, --threads``          Number of parallel threads used
``--input-format``         Format of input files (csv, tsv, avro, shp, json, etc)
```--index``               Specify a particular GeoMesa index to write to, instead of all indices
``--temp-path``            A temporary path to write the output. When using Accumulo on S3, it may be faster to write the
                           output to HDFS first using this parameter
``--no-tracking``          This application closes when ingest job is submitted. Note that this will require manual import
                           of the resulting RFiles.
``--run-mode``             Must be ``distributed`` for bulk ingest
``--split-max-size``       Maximum size of a split in bytes (distributed jobs)
``--src-list``             Input files are text files with lists of files, one per line, to ingest
``--skip-import``          Generate the RFiles but skip the bulk import into Accumulo
``--force``                Suppress any confirmation prompts
``<files>...``             Input files to ingest
========================== ==================================================================================================

The ``--output`` directory will be interpreted as a distributed file system path. If it already exists, the user will
be prompted to delete it before running the ingest.

The ``--cql`` parameter is required if using a partitioned schema (see :ref:`partitioned_indices` for details).
The filter must cover the partitions for all the input data, so that the partition tables can be
created appropriately. Any feature which doesn't match the filter or correspond to a an existing
table will fail to be ingested.

``--skip-import`` can be used to skip the import of the RFiles into Accumulo. The files can be imported later
through the ``importdirectory`` command in the Accumulo shell. Note that if ``--no-tracking`` is specified,
the import will be skipped regardless.

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

This command is particularly useful when using :ref:`accumulo_feature_expiry`, to ensure that expired rows are
physically deleted from disk. In this scenario, the ``--from`` parameter should be set to the age-off period, and
the ``--duration`` parameter should be set based on how often compactions are run. The intent is to only compact
the data that may have aged-off since the last compaction. Note that the time periods align with attribute-based
age-off; ingest time age-off may need a time buffer, assuming some relationship between ingest time and the default
date attribute.

This command can also be used to speed up queries by removing entries that are duplicated or marked for deletion.
This may be useful for a static data set, which will not be automatically compacted by Accumulo once the size
stops growing. In this scenario, the ``--from`` and ``--duration`` parameters can be omitted, so that the
entire data set is compacted.

.. _accumulo_age_off_command:

``configure-age-off``
^^^^^^^^^^^^^^^^^^^^^

List, add or remove age-off on a given feature type. See :ref:`accumulo_feature_expiry` for more information.

.. warning::

  Any manually configured age-off iterators should be removed before using this command, as they may
  not operate correctly due to the configuration name.

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

The ``--list`` argument will display any configured age-off::

  $ geomesa-accumulo configure-age-off -c test_catalog -f test_feature --list
  INFO  Attribute age-off: None
  INFO  Timestamp age-off: name:age-off, priority:10, class:org.locationtech.geomesa.accumulo.iterators.AgeOffIterator, properties:{retention=PT1M}

The ``--remove`` argument will remove any configured age-off::

  $ geomesa-accumulo configure-age-off -c test_catalog -f test_feature --remove

The ``--set`` argument will configure age-off. This will remove any existing age-off configuration and replace it
with the new specification. When using ``--set``, ``--expiry`` must also be provided. ``--expiry`` can be any time
duration string, specified in natural language.

If ``--dtg`` is provided, age-off will be based on the specified date-type attribute::

  $ geomesa-accumulo configure-age-off -c test_catalog -f test_feature --set --expiry '1 day' --dtg my_date_attribute

Otherwise, age-off will be based on ingest time::

  $ geomesa-accumulo configure-age-off -c test_catalog -f test_feature --set --expiry '1 day'

.. warning::

    Ingest time expiration requires that logical timestamps are disabled in the schema. See
    :ref:`logical_timestamps` for more information.

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
