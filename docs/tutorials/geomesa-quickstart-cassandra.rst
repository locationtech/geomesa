GeoMesa Cassandra Quick Start
=============================

This tutorial is the fastest and easiest way to get started with GeoMesa using Cassandra.
It is a good stepping-stone on the path to the other tutorials, that present increasingly
involved examples of how to use GeoMesa.

About this Tutorial
-------------------

In the spirit of keeping things simple, the code in this tutorial only
does a few small things:

1. Establishes a new (static) SimpleFeatureType
2. Prepares the Cassandra tables to store this type of data
3. Creates a few thousand example SimpleFeatures
4. Writes these SimpleFeatures to Cassandra
5. Queries for a given geographic rectangle, time range, and attribute
   filter, writing out the entries in the result set
6. Uses GeoServer to visualize the data (optional)

Prerequisites
-------------

Before you begin, you must have the following installed and configured:

-  `Java <http://java.oracle.com/>`__ JDK 1.8
-  Apache `Maven <http://maven.apache.org/>`__ |maven_version|
-  a GitHub client
-  a Cassandra |cassandra_version| instance, either standalone or cluster
-  a Cassandra user that has both create-table and write permissions
   (not needed for standalone instances)

Create A Cassandra Namespace
----------------------------

You will need a namespace in Cassandra to contain the tutorial tables. The easiest way to do
this is with the ``cqlsh`` tool provided with Cassandra distributions. Start ``cqlsh``, then type:

.. code-block:: bash

    cqlsh>  CREATE KEYSPACE geomesa WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

This creates a key space called "geomesa". This is a top-level name
space within Cassandra and it will provide a place for GeoMesa to put
all of its data, including data for spatial features and associated
metadata.

Download and Build the Tutorial
-------------------------------

Pick a reasonable directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-tutorials.git
    $ cd geomesa-tutorials

.. warning::

    Make sure that you download or checkout the version of the tutorials project that corresponds to
    your GeoMesa version. See :ref:`tutorial_versions` for more details.

To ensure that the quick start works with your environment, modify the ``pom.xml``
to set the appropriate versions for Cassandra, etc.

For ease of use, the project builds a bundled artifact that contains all the required
dependencies in a single JAR. To build, run:

.. code-block:: bash

    $ mvn clean install -pl geomesa-tutorials-cassandra/geomesa-tutorials-cassandra-quickstart -am

Running the Tutorial
--------------------

On the command line, run:

.. code-block:: bash

    $ java -cp geomesa-tutorials-cassandra/geomesa-tutorials-cassandra-quickstart/target/geomesa-tutorials-cassandra-quickstart-$VERSION.jar \
        org.geomesa.example.cassandra.CassandraQuickStart \
        --cassandra.contact.point <host:port>             \
        --cassandra.keyspace <keyspace>                   \
        --cassandra.catalog <table>                       \
        --cassandra.username <user>                       \
        --cassandra.password <password>

where you provide the following arguments:

- ``<host:port>`` the hostname and port your Cassandra instance is
  running on. For Cassandra standalone this will be ``localhost:9042``. More
  information on how to find your connection is available
  `here <http://www.geomesa.org/documentation/user/cassandra/install.html#connecting-to-cassandra>`__.
- ``<keyspace>`` keyspace your table will be put into. If you followed the instructions above,
  this will be ``geomesa``. More information on how to setup keyspaces is available
  `here <http://www.geomesa.org/documentation/user/cassandra/install.html#connecting-to-cassandra>`__.
- ``<table>`` the name of the destination table that will
  accept these test records. This table should either not exist or
  should be empty
- ``<user>`` (optional) the name of a Cassandra user that has
  permissions to create, read and write tables
- ``<password>`` (optional) the password for the previously-mentioned
  Cassandra user

Optionally, you can also specify that the quick start should delete its data upon completion. Use the
``--cleanup`` flag when you run to enable this behavior.

Once run, you should see the following output:

.. code-block:: none

    Loading datastore

    Creating schema: GLOBALEVENTID:String,Actor1Name:String,Actor1CountryCode:String,Actor2Name:String,Actor2CountryCode:String,EventCode:String,NumMentions:Integer,NumSources:Integer,NumArticles:Integer,ActionGeo_Type:Integer,ActionGeo_FullName:String,ActionGeo_CountryCode:String,dtg:Date,geom:Point

    Generating test data

    Writing test data
    Wrote 2356 features

    Running test queries
    Running query BBOX(geom, -120.0,30.0,-75.0,55.0) AND dtg DURING 2017-12-31T00:00:00+00:00/2018-01-02T00:00:00+00:00
    01 719024896=719024896|UNITED STATES|USA|SENATE||042|2|1|2|2|Texas, United States|US|2017-12-31T00:00:00.000Z|POINT (-97.6475 31.106)
    02 719024888=719024888|SENATE||UNITED STATES|USA|043|2|1|2|2|Texas, United States|US|2017-12-31T00:00:00.000Z|POINT (-97.6475 31.106)
    03 719024892=719024892|UNITED STATES|USA|DEPUTY||010|4|1|4|3|Abbeville, South Carolina, United States|US|2017-12-31T00:00:00.000Z|POINT (-82.379 34.1782)
    04 719024891=719024891|UNITED STATES|USA|||010|2|1|2|3|Ninety Six, South Carolina, United States|US|2017-12-31T00:00:00.000Z|POINT (-82.024 34.1751)
    05 719024894=719024894|UNITED STATES|USA|DEPUTY||010|2|1|2|3|Abbeville County, South Carolina, United States|US|2017-12-31T00:00:00.000Z|POINT (-82.4665 34.2334)
    06 719024887=719024887|DEPUTY||||010|4|1|4|3|Abbeville County, South Carolina, United States|US|2017-12-31T00:00:00.000Z|POINT (-82.4665 34.2334)
    07 719024893=719024893|UNITED STATES|USA|DEPUTY||010|6|1|6|3|Abbeville County, South Carolina, United States|US|2017-12-31T00:00:00.000Z|POINT (-82.4665 34.2334)
    08 719024895=719024895|UNITED STATES|USA|EMPLOYEE||010|2|1|2|3|Ninety Six, South Carolina, United States|US|2017-12-31T00:00:00.000Z|POINT (-82.024 34.1751)
    09 719024889=719024889|SENATE||UNITED STATES|USA|043|2|1|2|3|Washington, District of Columbia, United States|US|2017-12-31T00:00:00.000Z|POINT (-77.0364 38.8951)
    10 719024897=719024897|UNITED STATES|USA|SENATE||042|2|1|2|3|Washington, District of Columbia, United States|US|2017-12-31T00:00:00.000Z|POINT (-77.0364 38.8951)

    Returned 669 total features

    Running query BBOX(geom, -120.0,30.0,-75.0,55.0) AND dtg DURING 2017-12-31T00:00:00+00:00/2018-01-02T00:00:00+00:00
    Returning attributes [GLOBALEVENTID, dtg, geom]
    01 719024888=719024888|2017-12-31T00:00:00.000Z|POINT (-97.6475 31.106)
    02 719024896=719024896|2017-12-31T00:00:00.000Z|POINT (-97.6475 31.106)
    03 719024892=719024892|2017-12-31T00:00:00.000Z|POINT (-82.379 34.1782)
    04 719024891=719024891|2017-12-31T00:00:00.000Z|POINT (-82.024 34.1751)
    05 719024887=719024887|2017-12-31T00:00:00.000Z|POINT (-82.4665 34.2334)
    06 719024893=719024893|2017-12-31T00:00:00.000Z|POINT (-82.4665 34.2334)
    07 719024895=719024895|2017-12-31T00:00:00.000Z|POINT (-82.024 34.1751)
    08 719024889=719024889|2017-12-31T00:00:00.000Z|POINT (-77.0364 38.8951)
    09 719024897=719024897|2017-12-31T00:00:00.000Z|POINT (-77.0364 38.8951)
    10 719024884=719024884|2017-12-31T00:00:00.000Z|POINT (-77.0364 38.8951)

    Returned 669 total features

    Running query EventCode = '051'
    01 719024909=719024909|||MELBOURNE|AUS|051|10|1|10|4|Melbourne, Victoria, Australia|AS|2018-01-01T00:00:00.000Z|POINT (144.967 -37.8167)
    02 719024963=719024963|||CITIZEN||051|6|2|6|4|City Of Sydney, New South Wales, Australia|AS|2018-01-01T00:00:00.000Z|POINT (151.217 -33.8833)
    03 719025168=719025168|AUSTRALIAN|AUS|||051|18|1|10|4|Sydney, New South Wales, Australia|AS|2018-01-01T00:00:00.000Z|POINT (151.217 -33.8833)
    04 719025178=719025178|AUSTRALIA|AUS|COMMUNITY||051|20|2|20|4|Sydney, New South Wales, Australia|AS|2018-01-01T00:00:00.000Z|POINT (151.217 -33.8833)
    05 719025965=719025965|MIDWIFE||||051|10|1|10|4|Sydney, New South Wales, Australia|AS|2018-01-01T00:00:00.000Z|POINT (151.217 -33.8833)
    06 719025248=719025248|BUSINESS||||051|10|1|10|1|Australia|AS|2018-01-01T00:00:00.000Z|POINT (135 -25)
    07 719025509=719025509|COMMUNITY||AUSTRALIA|AUS|051|2|1|2|1|Australia|AS|2018-01-01T00:00:00.000Z|POINT (135 -25)
    08 719025555=719025555|DENMARK|DNK|||051|2|1|2|1|Australia|AS|2018-01-01T00:00:00.000Z|POINT (135 -25)
    09 719025634=719025634|FIJI|FJI|||051|2|1|2|1|Fiji|FJ|2018-01-01T00:00:00.000Z|POINT (178 -18)
    10 719025742=719025742|KING||||051|22|3|22|3|San Diego, California, United States|US|2018-01-01T00:00:00.000Z|POINT (-117.157 32.7153)

    Returned 138 total features

    Running query EventCode = '051' AND dtg DURING 2017-12-31T00:00:00+00:00/2018-01-02T00:00:00+00:00
    Returning attributes [GLOBALEVENTID, dtg, geom]
    01 719024909=719024909|2018-01-01T00:00:00.000Z|POINT (144.967 -37.8167)
    02 719024963=719024963|2018-01-01T00:00:00.000Z|POINT (151.217 -33.8833)
    03 719025168=719025168|2018-01-01T00:00:00.000Z|POINT (151.217 -33.8833)
    04 719025178=719025178|2018-01-01T00:00:00.000Z|POINT (151.217 -33.8833)
    05 719025965=719025965|2018-01-01T00:00:00.000Z|POINT (151.217 -33.8833)
    06 719025248=719025248|2018-01-01T00:00:00.000Z|POINT (135 -25)
    07 719025509=719025509|2018-01-01T00:00:00.000Z|POINT (135 -25)
    08 719025555=719025555|2018-01-01T00:00:00.000Z|POINT (135 -25)
    09 719025634=719025634|2018-01-01T00:00:00.000Z|POINT (178 -18)
    10 719025742=719025742|2018-01-01T00:00:00.000Z|POINT (-117.157 32.7153)

    Returned 138 total features

    Done

Looking at the Code
-------------------

The source code is meant to be accessible for this tutorial. The main logic is contained in
the generic ``org.geomesa.example.quickstart.GeoMesaQuickStart`` in the ``geomesa-tutorials-common`` module,
which is datastore agnostic. Some relevant methods are:

-  ``createDataStore`` get a datastore instance from the input configuration
-  ``createSchema`` create the schema in the datastore, as a pre-requisite to writing data
-  ``writeFeatures`` use a ``FeatureWriter`` to write features to the datastore
-  ``queryFeatures`` run several queries against the datastore
-  ``cleanup`` delete the sample data and dispose of the datastore instance

The quickstart uses a small subset of GDELT data. Code for parsing the data into GeoTools SimpleFeatures is
contained in ``org.geomesa.example.data.GDELTData``:

-  ``getSimpleFeatureType`` creates the ``SimpleFeatureType`` representing the data
-  ``getTestData`` parses an embedded TSV file to create ``SimpleFeature`` objects
-  ``getTestQueries`` illustrates several different query types, using CQL (GeoTools' Contextual Query Language)

Visualize Data (optional)
-------------------------

There are two options to visual the data ingested by this quick start. The easiest option is to use the
``export`` command of the GeoMesa Cassandra tools distribution. For a more production ready example,
you can alternatively stand up a GeoServer and connect it to your Cassandra instance.

Visualize Data With Leaflet
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

    To successfully run this command you must have a computer that is connected to the internet
    in order to access external Leaflet resources.


The ``export`` command is a part of the GeoMesa Cassandra command-line tools. In order to use the
command, ensure you have the command-line tools installed as described in
:ref:`setting_up_cassandra_commandline`. The ``export`` command provides the ``leaflet`` format which
will export the features to a Leaflet map that you can open in your web browser. To produce the map,
run the following command from the GeoMesa Cassandra tools distribution directory:

.. code:: bash

    bin/geomesa-cassandra export    \
        --output-format leaflet     \
        --contact-point <host:port> \
        --key-space <keyspace>      \
        --catalog <table>           \
        --user <user>               \
        --password <password>


Where the connection parameters are the same you used above during the quickstart. To view the map simply
open the url provided by the command in your web browser. If you click the menu in the upper right of the
map you can enable and disable the heatmap and feature layers as well as the two provided base layers.

.. figure:: _static/geomesa-quickstart-gdelt-data/leaflet-layer-preview.png
    :alt: Visualizing quick-start data with Leaflet

    Visualizing quick-start data with Leaflet


Visualize Data With GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use GeoServer to access and visualize the data stored in GeoMesa. In order to use GeoServer,
download and install version |geoserver_version|. Then follow the instructions in
:ref:`install_cassandra_geoserver` to enable GeoMesa.

Register the GeoMesa Store with GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Log into GeoServer using your user and password credentials. Click
"Stores" and "Add new Store". Select the ``Cassandra (GeoMesa)`` vector data
source, and fill in the required parameters.

Basic store info:

-  ``workspace`` this is dependent upon your GeoServer installation
-  ``data source name`` pick a sensible name, such as ``geomesa_quick_start``
-  ``description`` this is strictly decorative; ``GeoMesa quick start``

Connection parameters:

-  these are the same parameter values that you supplied on the
   command line when you ran the tutorial; they describe how to connect
   to the Cassandra instance where your data reside

Click "Save", and GeoServer will search your Cassandra table for any
GeoMesa-managed feature types.

Publish the Layer
~~~~~~~~~~~~~~~~~

GeoServer should recognize the ``gdelt-quickstart`` feature type, and
should present that as a layer that can be published. Click on the
"Publish" link.

You will be taken to the "Edit Layer" screen. You will need to enter values for the data bounding
boxes. In this case, you can click on the link to compute these values from the data.

Click on the "Save" button when you are done.

Take a Look
~~~~~~~~~~~

Click on the "Layer Preview" link in the left-hand gutter. If you don't
see the quick-start layer on the first page of results, enter the name
of the layer you just created into the search box, and press
``<Enter>``.

Once you see your layer, click on the "OpenLayers" link, which will open
a new tab. You should see a collection of red dots similar to the following image:

.. figure:: _static/geomesa-quickstart-gdelt-data/geoserver-layer-preview.png
    :alt: Visualizing quick-start data with GeoServer

    Visualizing quick-start data with GeoServer

Tweaking the display
~~~~~~~~~~~~~~~~~~~~

Here are just a few simple ways you can play with the visualization:

-  Click on one of the red points in the display, and GeoServer will
   report the detail records underneath the map area.
-  Shift-click to highlight a region within the map that you would like
   to zoom into.
-  Click on the "Toggle options toolbar" icon in the upper-left corner
   of the preview window. The right-hand side of the screen will include
   a "Filter" text box. Enter ``EventCode = '051'``, and press on the
   "play" icon. The display will now show only those points matching
   your filter criterion. This is a CQL filter, which can be constructed
   in various ways to query your data. You can find more information
   about CQL from `GeoServer's CQL
   tutorial <http://docs.geoserver.org/2.9.1/user/tutorials/cql/cql_tutorial.html>`__.
