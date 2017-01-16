Cassandra Command Line Tools
============================

The GeoMesa Cassandra distribution comes with a set of command line tools
for working with GeoMesa and Cassandra. This section shows how to use
the tools.

Before starting, check the following:

- Make sure Cassandra is running (check by running ``bin/nodetool status`` in your
  Cassandra installation directory.)
- Make sure you've set the ``CASSANDRA_LIB`` environment variable (check by running
  ``echo $CASSANDRA_LIB``)
- Make sure you've created a key space within Cassandra (check by starting ``bin/cqlsh`` in your
  Cassandra installation directory, and then by typing ``DESCRIBE KEYSPACES``)

Then, ``cd`` into the ``GEOMESA_CASSANDRA_HOME`` directory, and type ``bin/geomesa-cassandra help``. You should
get a listing of the tools with descriptions.

The first tool we'll try is the ``ingest`` command. This command takes data from an external source and
ingests it into the Cassandra database. For this example, we'll use some sample data located
at ``examples/ingest/csv/example.csv`` in the ``GEOMESA_CASSANDRA_HOME`` directory. Also, to make
editing and running the command easier, we'll use the example bash script at ``examples/ingest/csv/ingest_csv.sh``.
This is the script::

    #!/usr/bin/env bash

    # spec and converter for example_csv are registered in $GEOMESA_CASSANDRA_HOME/conf/application.conf
    bin/geomesa-cassandra ingest \
        --contact-point 127.0.0.1:9042 \
        --key-space mykeyspace \
        --catalog mycatalog \
        --name-space mynamespace \
        --converter example_csv \
        --spec example_csv \
        examples/ingest/csv/example.csv

- The ``contact-point`` is the address a Cassandra node. If you're running Cassandra locally with
  the default configuration, you shouldn't have to change this.
- The ``key-space`` is the Cassandra key space that you created as part of configuring Cassandra (see above).
  Edit this argument if needed to match the name of your key space.
- The ``catalog`` argument specifies the name of a Cassandra table that will be created in your name space
  by the ``ingest`` command.
  You can name the catalog table anything you want.
  This table is a metadata table that contains a list of each GeoMesa data table
  that you have created. Before you run the ``ingest`` command for the first time, this table does
  not exist. When you run ``ingest`` for the first time, GeoMesa creates two tables: this "catalog" metadata
  table, and the table that actually stores your data. If you run ``ingest`` again with a different dataset,
  GeoMesa will append a row to the "catalog" table, and also create an additional table for the new data.
- The ``name-space`` parameter can be anything you want. (It is used by GeoTools to provide a name space for your feature types.)
- The ``converter`` and ``spec`` parameters refer to a specific file located at ``conf/application.conf`` within the
  ``GEOMESA_CASSANDRA_HOME`` directory. This file contains specifications for "converters" and "sfts" (SimpleFeatureTypes) parsed by the :ref:`converters` library.
  In this specific example it only contains one of each: a converter and a SimpleFeatureType which are both called
  "example_csv". The converter specifies how a raw data file should be parsed. For example, the converter specifies
  that the fourth column of the input file should be converted to a date using a specific date format. The
  SimpleFeatureType part specifies how the parsed data should be used to create a SimpleFeatureType. For example, this
  files specifies that the SimpleFeatureType's first attribute should be one called "name". GeoMesa uses these specifications
  to ingest the data from the external data file into the Cassandra database. (For more information on converters see :ref:`converters`.)

  GeoMesa automatically finds the ``conf/application.conf`` file based on its name and location. You can add additional
  converters and SFT specifications to it as needed for ingesting other datasets. In addition, you can also
  specify converters and SFT specifications by adding directories to the
  ``conf/sfts`` directory. For more details see :ref:`installing_sft_and_converter_definitions`.
- The last argument is the location of the data file that we want to ingest.

If needed, edit the parameter arguments in the bash script. Then, ``cd`` into the ``GEOMESA_CASSANDRA_HOME``
directory, and run the script by typing::

  source examples/ingest/csv/ingest_csv.sh

You should see a message indicating that three features have been ingested.

.. note::

    If you see an error message regarding ``SLF4J``, find the ``logback-classic-1.1.3.jar``
    file in your ``CASSANDRA_LIB`` directory, and rename it to include a ``.exclude`` extension.

You can take a look at what happened
by going to the Cassadra CQL shell (``cqlsh``) and typing::

  DESCRIBE KEYSPACE mykeyspace ;

This will show that two new tables have been created: ``mycatalog`` and ``example_csv``. Type::

  SELECT * FROM mykeyspace.mycatalog ;

and ::

  SELECT * FROM mykeyspace.example_csv ;

to see the contents of the tables.

Now that we've ingested some data into the Cassandra database, we can try using some other commands. For example,
we list the tables ("feature types") that we've ingested::

    bin/geomesa-cassandra get-type-names \
        --contact-point 127.0.0.1:9042 \
        --key-space mykeyspace \
        --catalog mycatalog \
        --name-space mynamespace \

We can also inspect the feature type that we just ingested::

    bin/geomesa-cassandra describe-schema \
        --contact-point 127.0.0.1:9042 \
        --key-space mykeyspace \
        --catalog mycatalog \
        --name-space mynamespace \
        --feature-name example_csv

Configuring the Command Line Tools
----------------------------------

You can configure the command line tools using the
``conf/geomesa-env.sh`` file in the ``GEOMESA_CASSANDRA_HOME`` directory.
See the comments in that file for instructions.

Ingesting Other Datasets
------------------------

To ingest other datasets, you need to provide converter and SimpleFeatureType specifications.
For details on how to provide these specifications, see :ref:`installing_sft_and_converter_definitions`
and :ref:`ingest`. For more details on the converter specification syntax see :ref:`converters`.

When ingesting other datasets, keep the following GeoMesa-Cassandra-specific limitations in mind:

- The feature type must have a date/time field in addition to a geometry field.
- The geometry type must be "Point". Polygons and other geometry types are not allowed.
- The following attribute names may not be used in the feature type specification: ``pkz``, ``z31``, and ``fid`` .
  However, any field in the original data may be chosen as the ID field. This field will become the
  ``fid`` table in the Cassandra table.
- The name of the feature type must be a valid Cassandra table name.
- Complex field types like lists and maps are not allowed.