GeoMesa Cassandra Quick Start
=============================

This tutorial is the fastest and easiest way to get started with
GeoMesa. It is a good stepping-stone on the path to the other tutorials
that present increasingly involved examples of how to use GeoMesa.

In the spirit of keeping things simple, the code in this tutorial only
does a few small things:

1. establishes a new (static) SimpleFeatureType
2. prepares the Cassandra table to store this type of data
3. creates 1000 SimpleFeatures
4. writes these SimpleFeatures to the Cassandra table
5. queries for a given geographic rectangle, time range, and attribute
   filter, writing out the entries in the result set

The only dynamic element in the tutorial is the Cassandra destination;
that is a property that you provide on the command-line when running the
code.

Prerequisites
-------------

Before you begin, you must have the following:

-  an instance of Cassandra |cassandra_version| standalone or cluster,
-  a Cassandra user that has both create-table and write permissions
   (not needed for Cassandra standalone versions),
-  a local copy of the `Java <http://java.oracle.com/>`__ JDK 8,
-  Apache `Maven <http://maven.apache.org/>`__ installed, and
-  a GitHub client installed.

Create A Cassandra Namespace
----------------------------

You will need a namespace in Cassandra for the tutorial to create tables
in. The Easiest way to do this is with the ``cqlsh`` tool provided with
Cassandra distributions. Start cqlsh, then type:

.. code-block:: bash

    cqlsh>  CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

This creates a key space called “mykeyspace”. This is a top-level name
space within Cassandra and it will provide a place for GeoMesa to put
all of its data, including data for spatial features and associated
metadata.

Download and Build the Tutorial
-------------------------------

Pick a reasonable directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-tutorials.git
    $ cd geomesa-tutorials

.. note::

    You may need to download a particular release of the
    tutorials project to target a particular GeoMesa release.

To build, run

.. code-block:: bash

    $ mvn clean install -pl geomesa-quickstart-cassandra

.. note::

    Ensure that the version of Cassandra, Hadoop, etc in
    the root ``pom.xml`` match your environment.

.. note::

    Depending on the version, you may also need to build
    GeoMesa locally. Instructions can be found
    `here <https://github.com/locationtech/geomesa/>`__.

About this Tutorial
-------------------

The QuickStart operates by inserting and then querying 1000 features.
After the insertions are complete, a sequence of queries are run to
demonstrate different types of queries possible via the GeoTools API.

Run the Tutorial
----------------

On the command-line, run:

.. code-block:: bash

    $ java -cp geomesa-quickstart-cassandra/target/geomesa-quickstart-cassandra-${geomesa.version}.jar \
      com.example.geomesa.cassandra.CassandraQuickStart \
      -contact_point <host:port>                        \
      -keyspace <keyspace>                              \
      -catalog_table <catalog_table>                    \
      -username <username>                              \
      -password <password>

where you provide the following arguments:

-  ``<host:port>`` the hostname and port your Cassandra instance is
   running on. For Cassandra standalong this is localhost:9042.\ `More
   info on how to find this information
   here <http://www.geomesa.org/documentation/user/cassandra/install.html#connecting-to-cassandra>`__
-  ``<keyspace>`` keyspace your table will be put into. `More info on
   how to setup keyspaces
   here <http://www.geomesa.org/documentation/user/cassandra/install.html#connecting-to-cassandra>`__
-  ``<catalog_table>`` the name of the destination table that will
   accept these test records; this table should either not exist or
   should be empty
-  ``<user>`` (optional) the name of a Cassandra user that has
   permissions to create, read and write tables
-  ``<password>`` (optional) the password for the previously-mentioned
   Cassandra user

You should see output similar to the following (not including some of
Maven's output and log4j's warnings):

::

    New Cassandra host /127.0.0.1:9042 added
    Creating feature-type (schema):  CassandraQuickStart
    Creating new features
    Inserting new features
    Submitting query
    1.  Bierce|640|Sun Sep 14 15:48:25 EDT 2014|POINT (-77.36222958792739 -37.13013846773835)|null
    2.  Bierce|886|Tue Jul 22 14:12:36 EDT 2014|POINT (-76.59795732474399 -37.18420917493149)|null
    3.  Bierce|925|Sun Aug 17 23:28:33 EDT 2014|POINT (-76.5621106573523 -37.34321201566148)|null
    4.  Bierce|589|Sat Jul 05 02:02:15 EDT 2014|POINT (-76.88146600670152 -37.40156607152168)|null
    5.  Bierce|394|Fri Aug 01 19:55:05 EDT 2014|POINT (-77.42555615743139 -37.26710898726304)|null
    6.  Bierce|931|Fri Jul 04 18:25:38 EDT 2014|POINT (-76.51304097832912 -37.49406125975311)|null
    7.  Bierce|322|Tue Jul 15 17:09:42 EDT 2014|POINT (-77.01760098223343 -37.30933767159561)|null
    8.  Bierce|343|Wed Aug 06 04:59:22 EDT 2014|POINT (-76.66826220670282 -37.44503877750368)|null
    9.  Bierce|259|Thu Aug 28 15:59:30 EDT 2014|POINT (-76.90122194030118 -37.148525741002466)|null
    Submitting secondary index query
    Feature ID Observation.859 | Who: Bierce
    Feature ID Observation.355 | Who: Bierce
    Feature ID Observation.940 | Who: Bierce
    Feature ID Observation.631 | Who: Bierce
    Feature ID Observation.817 | Who: Bierce
    Submitting secondary index query with sorting (sorted by 'What' descending)
    Feature ID Observation.999 | Who: Addams | What: 999
    Feature ID Observation.996 | Who: Addams | What: 996
    Feature ID Observation.993 | Who: Addams | What: 993
    Feature ID Observation.990 | Who: Addams | What: 990
    Feature ID Observation.987 | Who: Addams | What: 987

The quick start code may also be run via Maven using the ``live-test``
profile:

.. code-block:: bash

    $ mvn -pl geomesa-quckstart-cassandra -Plive-test exec:exec -Dcontact_point=<host:port> -Dkeyspace=<keyspace> -Dcatalog_table=<catalog_table> -Dusername=<username> -Dpassword=<password> 

Similarly, the ``username`` and ``password`` are not required here and
if you are using a Cassandra standalone instance with the example
namespace setup in "`Connecting to
Cassandra <http://www.geomesa.org/documentation/user/cassandra/install.html#connecting-to-cassandra>`__\ "
then you may simply run:

.. code-block:: bash

    $ mvn -pl geomesa-quckstart-cassandra -Plive-test exec:exec

Looking at the Code
-------------------

The source code is meant to be accessible for this tutorial, but here is
a high-level breakdown of the methods in the ``CassandraQuickStart``
class that are relevant:

-  ``getCommonRequiredOptions`` helper code to establish the
   command-line parser for Cassandra options
-  ``getCassandraDataStoreConf`` create a ``HashMap`` of Cassandra
   parameters that will be used to fetch a ``DataStore``
-  ``createSimpleFeatureType`` defines the custom ``FeatureType`` used
   in the tutorial. There are five fields: Who, What, When, Where, and
   Why.
-  ``createNewFeatures`` creates a collection of new features, each of
   which is initialized to some randomized set of values
-  ``insertFeatures`` instructs the ``DataStore`` to write the
   collection of new features to the GeoMesa-managed Cassandra table
-  ``createFilter`` given a set of geometric bounds, temporal bounds,
   and an optional attribute-only expression, construct a common query
   language (CQL) filter that embodies these constraints. This filter
   will be used to query data.
-  ``queryFeatures`` query for records; for each, print out the five
   field (attribute) values
-  ``secondaryIndexExample`` additional examples that build other CQL
   queries
-  ``main`` this is the main entry point; it collects command-line
   parameters, builds the ``DataStore``, creates and inserts new
   records, and then kicks off the queries

Visualize Data With GeoServer
-----------------------------

Register the GeoMesa store with GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Log into GeoServer using your user and password credentials. Click
"Stores" and "Add new Store". If you do not see the Cassandra Feature
Data Store listed under Vector Data Sources, ensure the plugin is in the
right directory and restart GeoServer. For instructions on how to
install the GeoMesa plugin for GeoServer see :ref:`install_cassandra_geoserver`.

Select the ``Cassandra (GeoMesa)`` vector data source, and enter the
following parameters:

Basic store info:

-  ``workspace`` this is dependent upon your GeoServer installation
-  ``data source name`` pick a sensible name, such as,
   ``geomesa_quick_start``
-  ``description`` this is strictly decorative; ``GeoMesa quick start``

Connection parameters:

-  these are the same parameter values that you supplied on the
   command-line when you ran the tutorial; they describe how to connect
   to the Cassandra instance where your data reside.
-  the ``geomesa.cassandra.username`` and ``geomesa.cassandra.password``
   are required fields here. If you are using the Cassandra standalone,
   use the default value of ``cassandra`` for both.

Click "Save", and GeoServer will search your Cassandra table for any
GeoMesa-managed feature types.

Publish the layer
~~~~~~~~~~~~~~~~~

GeoServer should recognize the ``CassandraQuickStart`` feature type, and
should present that as a layer that could be published. Click on the
"Publish" link.

You will be taken to the Edit Layer screen. Two of the tabs need to be
updated: Data and Dimensions.

In the Data pane, enter values for the bounding boxes. In this case, you
can click on the link to compute these values from the data.

In the Dimensions tab, check the "Enabled" checkbox under Time. Then
select "When" in the Attribute and End Attribute dropdowns, and
"Continuous Interval" in the Presentation dropdown.

Click on the "Save" button when you are done.

Take a look
~~~~~~~~~~~

Click on the "Layer Preview" link in the left-hand gutter. If you don't
see the quick-start layer on the first page of results, enter the name
of the layer you just created into the search box, and press
``<Enter>``.

Once you see your layer, click on the "OpenLayers" link, which will open
a new tab. By default, the display that opens will not show all the
data, because we have enabled the time dimension for this layer, but the
preview does not specify a time. In the URL bar for the visualization,
add the following to the end:

``&TIME=2014-01-01T00:00:00.000Z/2014-01-31T23:59:59.999Z``

That tells GeoServer to display the records for the entire month of
January 2014. You can find more information about the TIME parameter
from `GeoServer's
documentation <http://docs.geoserver.org/2.9.1/user/services/wms/time.html>`__.

Once you press ``<Enter>``, the display will update, and you should see
a collection of red dots similar to the following image.

.. figure:: _static/geomesa-quickstart-hbase/geoserver-layer-preview.png
   :alt: Visualizing quickstart data

   Visualizing quickstart data

Tweaking the display
~~~~~~~~~~~~~~~~~~~~

Here are just a few simple ways you can play with the visualization:

-  Click on one of the red points in the display, and GeoServer will
   report the detail records underneath the map area.
-  Shift-click to highlight a region within the map that you would like
   to zoom into.
-  Alter the ``TIME=`` parameter in the URL to a different date range,
   and you can filter to see only the records that satisfy the temporal
   constraint.
-  Click on the "Toggle options toolbar" icon in the upper-left corner
   of the preview window. The right-hand side of the screen will include
   a "Filter" text box. Enter ``Who = 'Bierce'``, and press on the
   "play" icon. The display will now show only those points matching
   your filter criterion. This is a CQL filter, which can be constructed
   in various ways to query our data. You can find more information
   about CQL from `GeoServer's CQL
   tutorial <http://docs.geoserver.org/2.9.1/user/tutorials/cql/cql_tutorial.html>`__.

