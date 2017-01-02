Cassandra Data Store
====================

The GeoMesa Cassandra Data Store is found in the ``geomesa-cassandra``
in the source distribution.

The GeoMesa Cassandra DataStore provides a GeoTools API backed by Cassandra for geospatial data.  Currently, the
implementation is very much alpha - we are actively soliciting help in making this a robust library for handling
geospatial data within Cassandra.  See the note below on things to do and ways you can help out.


Getting started with Cassandra and GeoMesa
------------------------------------------

Installing and Configuring Cassandra and GeoMesa
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The first step to getting started with Cassandra and GeoMesa is to install
Cassandra itself. You can find good directions for downloading and installing
Cassandra online. For example, see Cassandra's official "Getting Started" documentation
here: https://cassandra.apache.org/doc/latest/getting_started/index.html .

Once you have Cassandra installed, the next step is to prepare your Cassandra installation
to integrate with GeoMesa. First, create a key space within Cassandra. The easiest way to
do this with ``cqlsh``, which should have been installed as part of your Cassandra installation.
Start ``cqlsh``, then type::

    CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

This creates a key space called "mykeyspace". This is a top-level name space within Cassandra
and it will provide a place for GeoMesa to put all of its data, including data for spatial features
and associated metadata.

Next, you'll need to set the ``CASSANDRA_LIB`` environment variable. GeoMesa uses this variable
to find the Cassandra jars. These jars should be in the ``lib`` directory of your Cassandra
installation. To set the variable add the following line to your ``.profile`` or ``.bashrc`` file::

    export CASSANDRA_LIB=/path/to/cassandra/lib

Finally, make sure you know a contact point for your Cassandra instance.
If you are just trying things locally, and using the default Cassandra settings,
the contact point would be ``127.0.0.1:9042``. You can check and configure the
port you are using using the ``native_transport_port`` in the Cassandra
configuration file (located at ``conf/cassandra.yaml`` in your Cassandra
installation directory).

After you've successfully installed and configured Cassandra, you can
move on to installing the GeoMesa Cassandra distribution.
To start, go to
https://repo.locationtech.org/content/repositories/geomesa-snapshots/org/locationtech/geomesa/geomesa-cassandra-dist_2.11/1.3.0-m3-SNAPSHOT/ ,
then find and download the most recent distribution archive and extract it to a convenient location.
For example,  assuming you are in the directory where you want to extract the distribution, you can type::

    wget https://repo.locationtech.org/content/repositories/geomesa-snapshots/org/locationtech/geomesa/geomesa-cassandra-dist_2.11/1.3.0-m3-SNAPSHOT/geomesa-cassandra-dist_2.11-1.3.0-m3-20170102.194727-88-bin.tar.gz
    tar xvf geomesa-cassandra-dist_2.11-1.3.0-m3-20170102.194727-88-bin.tar.gz

(Make sure to replace the archive name in the commands above with the file name for the most recent snapshot.)

The directory that you just extracted is the ``GEOMESA_CASSANDRA_HOME``. It includes all the files
that we'll need to use GeoMesa with Cassandra.

Getting Started with the GeoMesa Cassandra Tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

The first tool we'll try is the ``ingest`` command. This command takes data from an external source, and
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
  ``GEOMESA_CASSANDRA_HOME`` directory. This file contains specifications for "converters" and "sfts" (SimpleFeatureTypes).
  In this specific example it only contains one of each: a converter and a SimpleFeatureType which are both called
  "example_csv". The converter specifies how a raw data file should be parsed. For example, the converter specifies
  that the fourth column of the input file should be converted to a date using a specific date format. The
  SimpleFeatureType part specifies how the parsed data should be used to create a SimpleFeatureType. For example, this
  files specifies that the SimpleFeatureType's first attribute should be one called "name". GeoMesa uses these specifications
  to ingest the data from the external data file into the Cassandra database.

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can configure the command line tools using the
``conf/geomesa-env.sh`` file in the ``GEOMESA_CASSANDRA_HOME`` directory.
See the comments in that file for instructions.


Ingesting Other Datasets
^^^^^^^^^^^^^^^^^^^^^^^^

To ingest other datasets, you need to provide converter and SimpleFeatureType specifications.
For details on how to provide these specifications, see :ref:`installing_sft_and_converter_definitions`
and :ref:`ingest`. For more details on the converter specification syntax see :doc:`convert`.

When ingesting other datasets, keep the following GeoMesa-Cassandra-specific limitations in mind:

- The feature type must have a date/time field in addition to a geometry field.
- The geometry type must be "Point". Polygons and other geometry types are not allowed.
- The following attribute names may not be used in the feature type specification: ``pkz``, ``z31``, and ``fid`` .
  However, any field in the original data may be chosen as the ID field. This field will become the
  ``fid`` table in the Cassandra table.
- The name of the feature type must be a valid Cassandra table name.
- Complex field types like lists and maps are not allowed.

Getting Started with Cassandra, GeoMesa, and GeoServer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To set up a Cassandra data store in GeoServer, see :ref:`install_cassandra_geoserver` and then
:ref:`create_cassandra_ds_geoserver`. Note that when you create the data store, you'll be asked
to specify a work space name. Remember this name because you'll need to use it when querying the
data store in GeoServer.

Once you have a Cassandra layer set up in GeoServer you can try some queries. One way to test queries against the
GeoServer layer is to submit HTTP requests for the WMS and WFS services. For example, assuming
you have ingested the ``example.csv`` dataset as described above and set it up as a layer in GeoServer, this request should return
a PNG image with a single dot::

    http://localhost:8080/geoserver/wms?styles=&bbox=-180,-90,180,90&layers=myworkspace:example_csv&cql_filter=bbox(geom, -101, 22.0, -100.0, 24.0, 'EPSG:4326') and lastseen between 2015-05-05T00:00:00.000Z and 2015-05-10T00:00:00.000Z&version=1.3&service=WMS&width=100&request=GetMap&height=100&format=image/png&crs=EPSG:4326

and this request should return a JSON dataset with a single feature::

    http://localhost:8080/geoserver/wfs?service=wfs&request=GetFeature&cql_filter=bbox(geom, -101, 22.0, -100.0, 24.0, 'EPSG:4326') and lastseen between 2015-05-05T00:00:00.000Z and 2015-05-10T00:00:00.000Z&outputFormat=application/json&typeNames=myworkspace:example_csv

Note that you should replace ``myworkspace`` in these queries with the name of the work space you're using in GeoServer.
Also remember that all queries to a Cassandra layer must include both a ``bbox`` component and a date/time ``between`` component
as part of the CQL filter.


Using the Cassandra DataStore Programmatically
----------------------------------------------

Since the Cassandra DataStore is just another Geotools DataStore, you can use it exactly as you would any other Geotools
DataStore such as the PostGIS DataStore or the Accumulo DataStore.  To get a connection to a Cassandra DataStore, use the ```DataStoreFinder```.

.. code-block:: java

    import com.google.common.collect.ImmutableMap;
    import org.geotools.data.DataStore;
    import org.geotools.data.DataStoreFinder;
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;

    import java.io.IOException;
    import java.util.Arrays;
    import java.util.Map;

    Map<String, ?> params = ImmutableMap.of(
       CassandraDataStoreParams.CONTACT_POINT().getName() , "127.0.0.1:9142",
       CassandraDataStoreParams.KEYSPACE().getName()      , "geomesa_cassandra",
       CassandraDataStoreParams.NAMESPACE().getName()     , "mynamespace",
       CassandraDataStoreParams.CATALOG().getName()       , "mycatalog");
    DataStore ds = DataStoreFinder.getDataStore(params);
    ds.createSchema(SimpleFeatureTypes.createType("test", "testjavaaccess", "foo:Int,dtg:Date,*geom:Point:srid=4326"));


Implementation Details
----------------------

Limitations
^^^^^^^^^^^

Currently, the Cassandra DataStore only supports point/time data.  Fortunately, the vast majority of high volume
spatio-temporal datasets are 'event' data which map directly to the supported data type.  Additionally, the Cassandra
DataStore expects queries to have bbox or 'polygon within' and time-between predicates.  Additional predicates on any
attribute are supported, but they are applied during a post-processing phase.  See the TODO section for how to
optimize these predicates.

Index Structure
^^^^^^^^^^^^^^^

The Cassandra DataStore has a 32-bit integer as the primary key and a 64 bit integer as the clustering key, each with
the following structure.

**Partition/Primary Key (pkz)**

+---------------+-------------------------------+
| Bytes 31...16 | Byte 15...0                   |
+===============+===============================+
| Epoch Week    | 10-bit Z2 packed into 16 bits |
+---------------+-------------------------------+


**Clustering Key (z31)**

+---------------+
| Bytes 63...0  |
+===============+
| Full Z3       |
+---------------+

The week number since the epoch is encoded in the upper 16 bits of the primary key and a 10 bit Z2 index is encoded
in the lower 16 bits of the primary key.  This results in 1024 (10 bit Z2) primary partition keys per week.  For example,
a spatio-temporal point with lon/lat `-75.0,35.0` and dtg `2016-01-01T00:00:00.000Z` would have a primary key of
`157286595`. In addition to the primary key, the Cassandra DataStore encodes a Z3 index into the secondary sort index.  The Z3 index interleaves the latitude, longitude, and seconds in the current week into a 64 bit long.  See the TODO section for an
item regarding parameterizing the periodicity (Epoch Week).

Query Planning
^^^^^^^^^^^^^^

In order to satisfy a spatio-temporal query, the Cassandra DataStore first computes all of the row-keys that intersect
the geospatial region as well as the temporal region.  Then, for each coarse geospatial region, the Cassandra DataStore
computes the Z3 intervals that cover the finer resolution spatio-temporal region.  It then issues a query for each
unique row and Z3 interval to get back the result sets.  Each result set is post-processed with any remaining
predicates on attributes.

How you can contribute
^^^^^^^^^^^^^^^^^^^^^^

Here's a list of items that we will be adding to optimize the Cassandra DataStore
  * Pushdown predicates - push attribute predicates down into Cassandra rather than applying them in the post-processing
    phase
  * Configurable periodicity - utilizing one-week bounds as the coarse temporal part of the row key is not optimal for
    all data sets and workloads.  Make the coarse temporal part of the primary key configurable - i.e. day, year, etc
  * Configurable precision in the z3 clustering key - the full resolution z3 index results in a lot of range scans.
    We can limit the range scans by accommodating lower precision z3 indexes and pruning false positives in a
    post-processing step.
  * Prune false positives with push-down predicates - if we add the latitude and longitude as columns, we can prune
    false positives by having two predicates - the first specifying the range on z3 and the second specifying the bounds on x and y.
  * Non-point geometries - support linestrings and polygons
