.. _fsds_metadata:

FileSystem Metadata
===================

The FileSystem data store (FSDS) stores metadata about partitions and data files, to avoid having to repeatedly
interrogate the filesystem. When a new data file is added or removed, an associated metadata entry will be created
to track the operation.

File System Persistence
-----------------------

By default, metadata information is stored as a change log in the ``metadata`` folder under the root path for the
FSDS. This is the simplest solution, as it does not require any additional infrastructure. However, the initial
time required to read the metadata may be a limitation when dealing with a large number of partitions.

If the number of metadata files grows too large, they may be reduced by using the :ref:`fsds_compact_command` or
:ref:`fsds_manage_metadata_command` command-line functions, and/or manually moved into sub-folders.

Relational Database Persistence
-------------------------------

Alternatively, metadata may be stored in a relational database through JDBC. See :ref:`fsds_metadata_config` for
information on how to configure the metadata. A relational database may be specified by using the name ``jdbc``,
and supports the following configuration options (required options are marked with ``*``):

============================= ===================================================================================
Key                           Description
============================= ===================================================================================
``jdbc.url *``                The JDBC connection URL, e.g. ``jdbc:postgresql://localhost/geomesa``
``jdbc.driver``               The fully-qualified name of a JDBC driver class, e.g. ``org.postgresql.Driver``
``jdbc.user``                 The database user used to create connections
``jdbc.password``             The password for the database user
``jdbc.pool.min-idle``        The minimum number of connections to keep idle in the database connection pool
``jdbc.pool.max-idle``        The maximum number of connections to keep idle in the database connection pool
``jdbc.pool.max-size``        The maximum size of the database connection pool
``jdbc.pool.fairness``        Enable fairness when retrieving from the database connection pool (``true`` or
                              ``false``)
``jdbc.pool.test-on-borrow``  Test connections when retrieving them from the database connection pool (``true``
                              or ``false``)
``jdbc.pool.test-on-create``  Test connections when initially creating them (``true`` or ``false``)
``jdbc.pool.test-while-idle`` Test idle connections in the database connection pool (``true`` or ``false``)
============================= ===================================================================================

Currently, Postgres and H2 are officially supported. Other databases may work, but have not been tested.
