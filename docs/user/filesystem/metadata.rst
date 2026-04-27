.. _fsds_metadata:

FileSystem Metadata
===================

The FileSystem data store (FSDS) stores metadata about partitions and data files, to avoid having to repeatedly
interrogate the filesystem. When a new data file is added or removed, an associated metadata entry will be created
to track the operation. There are two supported metadata options:

File System Persistence
-----------------------

Metadata information can be stored in a file under the root path for the FSDS. This is the simplest solution, as it does not
require any additional infrastructure. However, the initial time required to read the metadata may be a limitation when dealing
with a large number of partitions.

The file-based metadata may be specified by using the name ``file``.

Relational Database Persistence
-------------------------------

Alternatively, metadata may be stored in a relational database through JDBC. A relational database may be
specified by using the name ``jdbc``, and supports the following configuration options, which can be specified
through the :ref:`fsds_parameters` ``fs.config.properties`` and ``fs.config.file`` (required options are marked with ``*``):

========================================= ===================================================================================
Key                                       Description
========================================= ===================================================================================
``fs.metadata.type *``                    Must be ``jdbc``. Can alternatively be specified through the data store parameter
                                          ``fs.metadata.type``
``fs.metadata.jdbc.url *``                The JDBC connection URL, e.g. ``jdbc:postgresql://localhost/geomesa``
``fs.metadata.jdbc.driver``               The fully-qualified name of a JDBC driver class, e.g. ``org.postgresql.Driver``
``fs.metadata.jdbc.user``                 The database user used to create connections
``fs.metadata.jdbc.password``             The password for the database user
``fs.metadata.jdbc.pool.min-idle``        The minimum number of connections to keep idle in the connection pool
``fs.metadata.jdbc.pool.max-idle``        The maximum number of connections to keep idle in the connection pool
``fs.metadata.jdbc.pool.max-size``        The maximum size of the connection pool
``fs.metadata.jdbc.pool.fairness``        Boolean to enable fairness when retrieving from the connection pool
``fs.metadata.jdbc.pool.test-on-borrow``  Boolean to enable testing connections when retrieved them from the connection pool
``fs.metadata.jdbc.pool.test-on-create``  Boolean to enable testing connections when initially creating them
``fs.metadata.jdbc.pool.test-while-idle`` Boolean to enable testing idle connections in the connection pool
========================================= ===================================================================================

Currently, only Postgres is officially supported. Other databases may work, but have not been tested.
