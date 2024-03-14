.. _kafka_quickstart:

GeoMesa Kafka Quick Start
=========================

This tutorial is the fastest and easiest way to get started with GeoMesa using Kafka for streaming data.
It is a good stepping-stone on the path to the other tutorials, that present increasingly
involved examples of how to use GeoMesa.

About this Tutorial
-------------------

In the spirit of keeping things simple, the code in this tutorial only
does a few small things:

1. Establishes a new (static) SimpleFeatureType
2. Prepares the Kafka topic to write this type of data
3. Creates a few thousand example SimpleFeatures
4. Writes these SimpleFeatures to the Kafka topic
5. Visualize the changing data in GeoServer (optional)
6. Creates event listeners for SimpleFeature updates (optional)

The quick start operates by simultaneously querying and writing several thousand feature updates.
The same feature identifier is used for each update, so there will only be a single "live" feature
at any one time.

The data used is from New York City taxi activity data published by the University
of Illinois. More information about the dataset is available `here <https://publish.illinois.edu/dbwork/open-data/>`__.

For this demo, only a single taxi is being tracked.

Background
----------

`Apache Kafka <https://kafka.apache.org/>`__ is "publish-subscribe
messaging rethought as a distributed commit log."

In the context of GeoMesa, Kafka is a useful tool for working with
streams of geospatial data. Interaction with Kafka in GeoMesa occurs
through the KafkaDataStore which implements the GeoTools
`DataStore <https://docs.geotools.org/latest/userguide/library/data/datastore.html>`__
interface.

Prerequisites
-------------

Before you begin, you must have the following installed and configured:

-  `Java <https://adoptium.net/temurin/releases/>`__ JDK 1.8
-  Apache `Maven <https://maven.apache.org/>`__ |maven_version|
-  a GitHub client
-  a Kafka |kafka_required_version| cluster

Ensure your Kafka and Zookeeper instances are running. You can use
Kafka's `quickstart <https://kafka.apache.org/documentation.html#quickstart>`__
to get Kafka/Zookeeper instances up and running quickly.

Configure GeoServer (optional)
------------------------------

You can use GeoServer to access and visualize the data stored in GeoMesa. In order to use GeoServer,
download and install version |geoserver_version|. Then follow the instructions in :ref:`install_kafka_geoserver`
to enable GeoMesa.

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
to set the appropriate versions for Kafka, Zookeeper, etc.

For ease of use, the project builds a bundled artifact that contains all the required
dependencies in a single JAR. To build, run:

.. code-block:: bash

    $ mvn clean install -pl geomesa-tutorials-kafka/geomesa-tutorials-kafka-quickstart -am

Running the Tutorial
--------------------

On the command line, run:

.. code-block:: bash

    $ java -cp geomesa-tutorials-kafka/geomesa-tutorials-kafka-quickstart/target/geomesa-tutorials-kafka-quickstart-$VERSION.jar \
        org.geomesa.example.kafka.KafkaQuickStart \
        --kafka.brokers <brokers>                 \
        --kafka.zookeepers <zookeepers>

where you provide the following arguments:

- ``<brokers>`` your Kafka broker instances, comma separated. For a
  local install, this would be ``localhost:9092``
- ``<zookeepers>`` your Zookeeper nodes, comma separated. For a local
  install, this would be ``localhost:2181``

Optionally, you can also specify that the quick start should delete its data upon completion. Use the
``--cleanup`` flag when you run to enable this behavior.

Once run, the quick start will create the Kafka topic, then pause and prompt you to register the layer in
GeoServer. If you do not want to use GeoServer, you can skip this step. Otherwise, follow the instructions in
the next section before returning here.

Once you continue, the tutorial should run for approximately thirty seconds. You should see the following output:

.. code-block:: none

    Loading datastore

    Creating schema: taxiId:String,dtg:Date,geom:Point

    Generating test data

    Feature type created - register the layer 'tdrive-quickstart' in geoserver with bounds: MinX[116.22366] MinY[39.72925] MaxX[116.58804] MaxY[40.09298]
    Press <enter> to continue

    Writing features to Kafka... refresh GeoServer layer preview to see changes
    Current consumer state:
    1277=1277|2008-02-03T04:32:53.000Z|POINT (116.35 39.90003)
    Current consumer state:
    1277=1277|2008-02-03T17:58:49.000Z|POINT (116.38812 39.93196)
    Current consumer state:
    1277=1277|2008-02-04T06:46:26.000Z|POINT (116.40218 39.94439)
    Current consumer state:
    1277=1277|2008-02-04T19:55:45.000Z|POINT (116.3631 39.94646)
    Current consumer state:
    1277=1277|2008-02-05T09:39:48.000Z|POINT (116.58264 40.07556)
    Current consumer state:
    1277=1277|2008-02-05T22:24:50.000Z|POINT (116.34112 39.95363)
    Current consumer state:
    1277=1277|2008-02-06T14:17:29.000Z|POINT (116.54203 39.91476)
    Current consumer state:
    1277=1277|2008-02-07T02:53:55.000Z|POINT (116.35683 39.89809)
    Current consumer state:
    1277=1277|2008-02-07T15:48:47.000Z|POINT (116.36785 39.99471)
    Current consumer state:
    1277=1277|2008-02-08T04:20:19.000Z|POINT (116.42872 39.91531)
    Current consumer state:
    1277=1277|2008-02-08T17:14:15.000Z|POINT (116.34609 39.93924)

    Done

Visualize Data With GeoServer (optional)
----------------------------------------

You can use GeoServer to access and visualize the data stored in GeoMesa. In order to use GeoServer,
download and install version |geoserver_version|. Then follow the instructions in :ref:`install_kafka_geoserver`
to enable GeoMesa.

Register the GeoMesa Store with GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Log into GeoServer using your user and password credentials. Click
"Stores" and "Add new Store". Select the ``Kafka (GeoMesa)`` vector data
source, and fill in the required parameters.

Basic store info:

-  ``workspace`` this is dependent upon your GeoServer installation
-  ``data source name`` pick a sensible name, such as ``geomesa_quick_start``
-  ``description`` this is strictly decorative; ``GeoMesa quick start``

Connection parameters:

-  these are the same parameter values that you supplied on the
   command line when you ran the tutorial; they describe how to connect
   to the Kafka instance where your data resides

Click "Save", and GeoServer will search Zookeeper for any GeoMesa-managed feature types.

Publish the Layer
~~~~~~~~~~~~~~~~~

If you have already run the command to start the tutorial, then GeoServer should recognize the
``tdrive-quickstart`` feature type, and should present that as a layer that can be published. Click on the
"Publish" link. If not, then run the tutorial as described above in **Running the Tutorial**. When
the tutorial pauses, go to "Layers" and "Add new Layer". Select the GeoMesa Kafka store you just
created, and then click "publish" on the ``tdrive-quickstart`` layer.

You will be taken to the Edit Layer screen. You will need to enter values for the data bounding
boxes. For this demo, use the values MinX: 116.22366, MinY: 39.72925, MaxX: 116.58804, MaxY: 40.09298.

Click on the "Save" button when you are done.

Take a Look
~~~~~~~~~~~

Click on the "Layer Preview" link in the left-hand gutter. If you don't
see the quick-start layer on the first page of results, enter the name
of the layer you just created into the search box, and press
``<Enter>``.

At first, there will be no data displayed. Once you have reached this
point, return to the quick start console and hit "<enter>" to continue the tutorial.
As the data is updated in Kafka, you can refresh the layer preview page to see
the feature moving around.

What's Happening in GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The layer preview of GeoServer uses the ``KafkaFeatureStore`` to show a
real time view of the current state of the data stream. There is a single
``SimpleFeature`` being updated over time in Kafka which is
reflected in the GeoServer display.

As you refresh the page, you should see the ``SimpleFeature`` move around.
Due to the nature of the taxi's routes, and the speed up of time in replaying
the data, there isn't much of a pattern to the movement.

Looking at the Code
-------------------

The source code is meant to be accessible for this tutorial. The logic is contained in
the generic ``org.geomesa.example.quickstart.GeoMesaQuickStart`` in the ``geomesa-quickstart-common`` module,
and the Kafka-specific ``org.geomesa.example.kafka.KafkaQuickStart`` in the ``geomesa-quickstart-kafka`` module.
Some relevant methods are:

-  ``createDataStore`` overridden in the ``KafkaQuickStart``  to use the input configuration to get a pair of datastore instances, one for writing and one for reading data.
-  ``createSchema`` create the schema in the datastore, as a pre-requisite to writing data
-  ``writeFeatures`` overridden in the ``KafkaQuickStart`` to simultaneously write and read features from Kafka
-  ``queryFeatures`` not used in this tutorial
-  ``cleanup`` delete the sample data and dispose of the datastore instance

The quickstart uses a small subset of taxi data. Code for parsing the data into GeoTools SimpleFeatures is
contained in ``org.geomesa.example.data.TDriveData``:

-  ``getSimpleFeatureType`` creates the ``SimpleFeatureType`` representing the data
-  ``getTestData`` parses an embedded CSV file to create ``SimpleFeature`` objects
-  ``getTestQueries`` not used in this tutorial

Listening for Feature Events (optional)
---------------------------------------

The GeoTools API also includes a mechanism to fire off a
`FeatureEvent <https://docs.geotools.org/stable/javadocs/org/geotools/api/data/FeatureEvent.Type.html>`__
each time there is an event in a ``DataStore`` (typically when the data is changed). A client may implement a
`FeatureListener <https://docs.geotools.org/stable/javadocs/org/geotools/api/data/FeatureListener.html>`__,
which has a single method called ``changed()`` that is invoked as each
``FeatureEvent`` is fired.

The code in ``KafkaListener`` implements a simple ``FeatureListener``
that prints the messages received. Open up a second terminal window and
run:

.. code-block:: bash

    $ java -cp geomesa-tutorials-kafka/geomesa-tutorials-kafka-quickstart/target/geomesa-tutorials-kafka-quickstart-$VERSION.jar \
        org.geomesa.example.kafka.KafkaListener \
        --kafka.brokers <brokers>               \
        --kafka.zookeepers <zookeepers>

Use the same settings for ``<brokers>`` and ``<zookeepers>`` that you did previously. Then
in the original terminal window, re-run the ``KafkaQuickStart`` code as
before. The ``KafkaListener`` terminal should produce messages like the
following:

.. code-block:: none

    Received FeatureEvent from schema 'tdrive-quickstart' of type 'CHANGED'
    1277=1277|2008-02-02T13:34:51.000Z|POINT (116.32674 39.89577)

The ``KafkaListener`` code will run until interrupted (typically with ctrl-c).

The portion of ``KafkaListener`` that creates and implements the
``FeatureListener`` is:

.. code-block:: java

    FeatureListener listener = featureEvent -> {
        System.out.println("Received FeatureEvent from schema '" + typeName + "' of type '" + featureEvent.getType() + "'");
        if (featureEvent.getType() == FeatureEvent.Type.CHANGED &&
            featureEvent instanceof KafkaFeatureChanged) {
            System.out.println(DataUtilities.encodeFeature(((KafkaFeatureChanged) featureEvent).feature()));
        } else if (featureEvent.getType() == FeatureEvent.Type.REMOVED) {
            System.out.println("Received Delete for filter: " + featureEvent.getFilter());
        }
    };
    datastore.getFeatureSource(typeName).addFeatureListener(listener);

(note the use of a lambda expression to create the listener)
