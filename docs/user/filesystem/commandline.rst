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

General Arguments
-----------------

Most commands require the ``--path`` argument, to specify the root storage path. Configuration properties can
be passed in using ``--config`` or ``--config-file``, which can be used to specify e.g. s3-related properties.

The ``--auths`` argument corresponds to the data store parameter ``geomesa.security.auths``. See
:ref:`data_security` for more information.

Commands
--------

.. _fsds_compact_command:

``compact``
^^^^^^^^^^^

Compact one or more filesystem partitions. This will merge multiple files into fewer, larger files, which may
provide better query performance.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
``--partitions``         Partitions to compact (omit to compact all partitions)
``--target-file-size``   Target size for data files (e.g. 500MB or 1GB)
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
``--partitions``         Partitions to operate on
``--no-header``          Suppress the column headers in the output
======================== =========================================================

At least one of ``--cql`` or ``--partitions`` must be specified, to select the partitions being operated on.

The results will be output in tab-delimited text, containing the partition name and the associated filter.

``get-files``
^^^^^^^^^^^^^

Displays the files for one or more filesystem partitions.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
``--partition``          Partitions to list (omit to list all partitions)
``-q, --cql``            CQL predicate to determine the partitions to operate on
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
``--partition-scheme``   Partition schemes
``--num-reducers``       Number of reducers to use (required for distributed ingest)
``--target-file-size``   Target size for data files (e.g. 500MB or 1GB)
``--temp-path``          Path to a temp directory used for working files
``--storage-opt``        Additional storage options to set as SimpleFeatureType user data, in the form ``key=value``
======================== =============================================================

If the schema does not already exist, then ``--partition-scheme`` is required, otherwise it may be omitted.

The ``--partition-scheme`` argument should be the well-known name of a provided partition scheme. See
:ref:`fsds_partition_schemes` for more information.

The ``--num-reducers`` should generally be set to half the number of partitions.

The ``--temp-path`` argument may be useful when working with ``s3`` data, as ``s3`` is slow to write to.

.. _fsds_manage_metadata_command:

``manage-metadata``
^^^^^^^^^^^^^^^^^^^

This command will compact, add and delete metadata entries in a file system storage instance. It has four
sub-commands:

* ``register`` - create a new metadata entry for an existing data file
* ``unregister`` - remove a metadata entry for an existing data file
* ``migrate`` - migrate metadata from one type to another
* ``check-consistency`` - check consistency between the metadata and data files

To invoke the command, use the command name followed by the sub-command, then any arguments. For example::

    $ geomesa manage-metadata compact -p /tmp/geomesa ...

======================== =============================================================
Argument                 Description
======================== =============================================================
``-p, --path *``         The filesystem root path used to store data
``-f, --feature-name *`` The name of the schema
======================== =============================================================

``register``
~~~~~~~~~~~~

The ``register`` sub-command will add metadata associated with a particular file. When new data files are created through some
external bulk process, then they must be registered using this command before they are queryable. Note that generally
files must already be in the same filesystem in order to be registered.

======================== =====================================================================================================
Argument                 Description
======================== =====================================================================================================
``<file> *``             The path of the file(s) to register
``--delete``             Delete files after copying them into the storage root path
======================== =====================================================================================================

``unregister``
~~~~~~~~~~~~~~

The ``unregister`` sub-command will the delete metadata associated with a particular file.

======================== =====================================================================================================
Argument                 Description
======================== =====================================================================================================
``<file> *``             The path of the file to unregister, relative to the storage root path
======================== =====================================================================================================

``migrate``
~~~~~~~~~~~~~~

The ``migrate`` sub-command will move the metadata storage from one type (``file`` or ``jdbc``) to another.

============================== ==============================================================================================
Argument                       Description
============================== ==============================================================================================
``--new-metadata-type *``      Metadata type to migrate to
``--new-metadata-config``      Metadata configuration properties for the type to migrate to, in the form k=v
``--new-metadata-config-file`` Name of a metadata configuration file for the type to migrate to, in Java properties format
============================== ==============================================================================================

``check-consistency``
~~~~~~~~~~~~~~~~~~~~~

The ``check-consistency`` sub-command will check the metadata against the data files. It will
find data files that are not referenced in the metadata, and metadata entries that do not
correspond to data files.

======================== ==================================================================
Argument                 Description
======================== ==================================================================
``--partition``          The name of partitions to check, or none for all partitions
``--threads, -t``        Number of threads to use when listing data files
======================== ==================================================================
