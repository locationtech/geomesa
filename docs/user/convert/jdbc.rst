.. _jdbc_converter:

JDBC Converter
==============

The JDBC converter allows you to create SimpleFeatures directly from a SQL select statement against an
existing database, using standard JDBC libraries. To use the JDBC converter, specify ``type = "jdbc"``
in your converter definition.

.. warning::

    The JDBC converter does not sanitize queries. Be careful with inputs, as a malicious actor could
    potentially execute SQL injection attacks.

Configuration
-------------

The JDBC connection string used to connect to the database must be specified using the ``connection`` element,
for example ``jdbc:mysql://localhost/test?user=foo&password=bar``.

The JDBC converter relies on standard JDBC libraries, and requires the correct JDBC driver for the database being
used. Ensure the correct driver is on the classpath; for GeoMesa binary distributions, it can be placed in the
``lib`` folder.

The JDBC converter takes SQL select statements as input. Because a select statement can return arbitrary columns,
it is import to ensure that the fields returned match the converter definition.

Transform Functions
-------------------

The ``transform`` element supports referencing the columns of the SQL result set through ``$`` notation.
Fields are identified by index as in a standard ``ResultSet`` (i.e. the first field is index 1, the second 2, etc).
The ``0`` index is a concatenated string of all other fields - this matches the behavior of file-based converters,
and can be useful for, e.g. generating a unique ID. Field values will have the native type binding of the database
column. For example, ``VARCHAR`` will be converted to ``String``, ``TIMESTAMP`` will become a ``java.sql.Date``, etc.

The JDBC converter also supports the standard transform functions; see :ref:`converter_functions` for
more details.

Command Line Ingestion
----------------------

Standard command line ingest expects a data file to operate on. You may place select statements in a file
(one per line), or you may use `stdin` to pipe the select statement to the ingest command::

    $ echo "select * from example limit 5" | geomesa ingest ... -c example_jdbc -C example-jdbc -s example-jdbc
    INFO  Creating schema example-jdbc
    INFO  Running ingestion in local mode
    INFO  Ingesting from stdin with 1 thread
    [============================================================] 100% complete 5 ingested 0 failed in 00:00:01
    INFO  Local ingestion complete in 00:00:01
    INFO  Ingested 5 features with no failures.

Example Usage
-------------

Assume the following table in MySQL::

    mysql> describe example;
    +-------+-------------+------+-----+-------------------+-----------------------------+
    | Field | Type        | Null | Key | Default           | Extra                       |
    +-------+-------------+------+-----+-------------------+-----------------------------+
    | id    | bigint(20)  | NO   | PRI | NULL              |                             |
    | name  | varchar(20) | YES  |     | NULL              |                             |
    | dtg   | timestamp   | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    | lat   | double      | YES  |     | NULL              |                             |
    | lon   | double      | YES  |     | NULL              |                             |
    +-------+-------------+------+-----+-------------------+-----------------------------+
    5 rows in set (0.00 sec)

The target simple feature type is defined as::

    "example-jdbc" = {
      attributes = [
        { name = "name", type = "String" }
        { name = "dtg",  type = "Date" }
        { name = "geom", type = "Point", srid = 4326, default = true }
      ]
    }

The converter is defined as::

    "example-jdbc" = {
      type       = "jdbc"
      connection = "jdbc:mysql://localhost/test?user=foo&password=bar"
      id-field   = "toString($1)",
      fields = [
        { name = "name",     transform = "$2"                }
        { name = "dtg",      transform = "$3"                }
        { name = "lon",      transform = "$4"                }
        { name = "lat",      transform = "$5"                }
        { name = "geom",     transform = "point($lon, $lat)" }
      ]
    }

And the input to the converter would be::

    "select * from example"

The required driver JAR would be::

    mysql-connector-java-5.1.44.jar
