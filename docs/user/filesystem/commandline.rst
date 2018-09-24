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

``compact``
^^^^^^^^^^^

Compact one or more filesystem partitions.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
``--partitions``         Partitions to compact (omit to compact all partitions)
``--mode``               One of ``local`` or ``distributed`` (to use map/reduce)
``--temp-path``          Path to a temp directory used for working files
======================== =========================================================

The ``--temp-path`` argument may be useful when working with ``s3`` data, as ``s3`` is slow to write to.

``get-files``
^^^^^^^^^^^^^

Displays the files for one or more filesystem partitions.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
``--partitions``         Partitions to compact (omit to list all partitions)
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
``-e, --encoding *``     The encoding used for the underlying files. Implementations are provided for ``parquet`` and ``orc``.
``--partition-scheme *`` Common partition scheme name (e.g. daily, z2) or path to a file containing a scheme config
``--num-reducers``       Number of reducers to use (required for distributed ingest)
``--leaf-storage``       Use leaf storage
``--temp-path``          Path to a temp directory used for working files
``--storage-opt``        Additional storage options, as ``key=value``
======================== =============================================================

The ``--partition-scheme`` argument should be the well-known name of a provided partition scheme, or the name
of a file containing a partition scheme. See :ref:`fsds_partition_schemes` for more information.

The ``--num-reducers`` should generally be set to half the number of partitions.

The ``--temp-path`` argument may be useful when working with ``s3`` data, as ``s3`` is slow to write to.

Example
~~~~~~~

Lets say that we have all our data for 2016 stored in an S3 bucket::

    $ geomesa-fs ingest \
       -p 's3a://mybucket/datastores/test' \
       -e parquet \
       --partition-scheme daily,z2-2bits \
       -s s3a://mybucket/schemas/my-config.conf \
       -C s3a://mybucket/schemas/my-config.conf \
       --temp-dir hdfs://namenode:port/tmp/gm/1 \
       --num-reducers 20 \
       's3a://mybucket/data/2016/*'

After ingest we expect to see a file structure with metadata and parquet files in S3 for our type named "myfeature"::

    aws s3 ls --recursive s3://mybucket/datastores/test

    datastores/test/myfeature/schema.sft
    datastores/test/myfeature/metadata
    datastores/test/myfeature/2016/01/01/0/0000.parquet
    datastores/test/myfeature/2016/01/01/2/0000.parquet
    datastores/test/myfeature/2016/01/01/3/0000.parquet
    datastores/test/myfeature/2016/01/02/0/0000.parquet
    datastores/test/myfeature/2016/01/02/1/0000.parquet
    datastores/test/myfeature/2016/01/02/3/0000.parquet

Two metadata files (``schema.sft`` and ``metadata``) store information about the schema, partition scheme, and list of
files that have been created. Note that the list of created files allows the datastore to quickly compute available
files to avoid possibly expensive directly listings against the filesystem. You may need to run ``update-metadata``
if you decide to insert new files.

Notice that the bucket "directory structure" includes year, month, day and then a 0,1,2,3 representing a quadrant of the
Z2 Space Filling Curve with 2bit resolution (i.e. 0 = lower left, 1 = lower right, 2 = upper left, 3 = upper right).
Note that in our example January 1st and 2nd both do not have all four quadrants represented. This means that the input
dataset for that day didn't have any data in that region of the world. If additional data were ingested, the directory
and a corresponding file would be created.

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
