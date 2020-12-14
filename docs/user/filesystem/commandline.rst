.. _fsds_tools:

FileSystem Command-Line Tools
=============================

The GeoMesa FileSystem distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`setting_up_fsds_commandline`.

Once installed, the tools should be available through the command ``geomesa-fs``::

    $ geomesa-fs
    INFO  Usage: geomesa-fs [command] [command options]
      Commands:
        ...

Commands that are common to multiple back ends are described in :doc:`/user/cli/index`. The commands
here are FileSystem-specific.

Commands
--------

.. _fsds_compact_command:

``compact``
^^^^^^^^^^^

Compact one or more filesystem partitions. This will merge multiple files into a single file, which may
provide better query performance.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
``--partitions``         Partitions to compact (omit to compact all partitions)
``--mode``               One of ``local`` or ``distributed`` (to use map/reduce)
``--temp-path``          Path to a temp directory used for working files
======================== =========================================================

The ``--temp-path`` argument may be useful when working with ``s3`` data, as ``s3`` is slow for incremental writes.

``generate-partition-filters``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Calculate filters that exactly match partitions. This can be used to facilitate exports from another system
directly into the appropriate partition directory.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL predicate to determine the partitions to operate on
``--partitions``         Partitions to operate on, subject to the CQL filter
``--no-header``          Suppress the column headers in the output
``--config``             Hadoop configuration properties, in the form key=value
======================== =========================================================

At least one of ``--cql`` or ``--partitions`` must be specified, to select the partitions being operated on.
If both are specified, any explicit partitions that don't match the CQL predicate will be ignored.

The results will be output in tab-delimited text. The following example uses the GDELT :ref:`fsds_quickstart`:

.. code-block:: bash

    $ ./geomesa-fs generate-partition-filters \
        -p hdfs://localhost:9000/fs           \
        -f gdelt-quickstart                   \
        -q "bbox(geom,0,0,180,90) and dtg during 2020-01-01T00:00:00.000Z/2020-01-03T00:00:00.000Z"
    INFO  Generating filters for 2 partitions
    Partition Path	Filter
    2020/01/01/3	hdfs://localhost:9000/fs/gdelt-quickstart/2020/01/01/3_I	BBOX(geom, 0.0,0.0,180.0,90.0) AND dtg >= '2020-01-01T00:00:00.000Z' AND dtg < '2020-01-02T00:00:00.000Z'
    2020/01/02/3	hdfs://localhost:9000/fs/gdelt-quickstart/2020/01/02/3_I	BBOX(geom, 0.0,0.0,180.0,90.0) AND dtg >= '2020-01-02T00:00:00.000Z' AND dtg < '2020-01-03T00:00:00.000Z'

``get-files``
^^^^^^^^^^^^^

Displays the files for one or more filesystem partitions.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
``--partitions``         Partitions to list (omit to list all partitions)
======================== =========================================================

``get-partitions``
^^^^^^^^^^^^^^^^^^

Displays the partitions for a given filesystem store.

======================== =============================================================
Argument                 Description
======================== =============================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
======================== =============================================================

.. _fsds_ingest_command:

``ingest``
^^^^^^^^^^

For an overview of ingestion options, see :ref:`cli_ingest`.

This command ingests files into a GeoMesa FS Datastore. Note that a "datastore" is simply a path in the filesystem.
All data and metadata will be stored in the filesystem under the hierarchy of the root path.

======================== =============================================================
Argument                 Description
======================== =============================================================
``-p, --path *``         The filesystem root path used to store data
``-e, --encoding``       The encoding used for the underlying files. Implementations are provided for ``parquet`` and ``orc``.
``--partition-scheme``   Common partition scheme name (e.g. daily, z2) or path to a file containing a scheme config
``--num-reducers``       Number of reducers to use (required for distributed ingest)
``--leaf-storage``       Use leaf storage
``--temp-path``          Path to a temp directory used for working files
``--storage-opt``        Additional storage options, as ``key=value``
======================== =============================================================

If the schema does not already exist, then ``--encoding`` and ``--partition-scheme`` are required, otherwise
they may be omitted.

The ``--partition-scheme`` argument should be the well-known name of a provided partition scheme, or the name
of a file containing a partition scheme. See :ref:`fsds_partition_schemes` for more information.

The ``--num-reducers`` should generally be set to half the number of partitions.

The ``--temp-path`` argument may be useful when working with ``s3`` data, as ``s3`` is slow to write to.

.. _fsds_manage_metadata_command:

``manage-metadata``
^^^^^^^^^^^^^^^^^^^

This command will compact, add and delete metadata entries in a file system storage instance. It has three
sub-commands:

* ``compact`` - compact multiple metadata files down to a single file
* ``register`` - create a new metadata entry for an existing data file
* ``unregister`` - remove a metadata entry for an existing data file

To invoke the command, use the command name followed by the sub-command, then any arguments. For example::

    $ geomesa manage-metadata compact -p /tmp/geomesa ...

======================== =============================================================
Argument                 Description
======================== =============================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
======================== =============================================================

``compact``
^^^^^^^^^^^

The ``compact`` sub-command will rewrite multiple metadata files as a single file. Note that this does
not change the data files; that is accomplished by the top-level ``compact`` command, as described above.

``register/unregister``
^^^^^^^^^^^^^^^^^^^^^^^

The ``register`` and ``unregister`` sub-commands will add or delete metadata associated with a particular file.
The files must already exist under the appropriate partition path. If new data files are created through some
external bulk process, then they must be registered using this command before they are queryable.

======================== =============================================================
Argument                 Description
======================== =============================================================
``--partition *``        The name of the partition to modify
``--files *``            The names of the files being registered. May be specified
                         multiple times to register multiple files
``--count``              The number of features in the files being registered. This
                         is not required, but can be used later for estimating query
                         sizes
``--bounds``             Geographic bounds of the data files being registered, in the
                         form ``xmin,ymin,xmax,ymax``. This is not required, but can
                         be used later for estimating query bounds
======================== =============================================================
