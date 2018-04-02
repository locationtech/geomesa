GeoMesa Lambda Quick Start
==========================

This tutorial can get you started with the GeoMesa Lambda data store. Note that the Lambda data store
is for advanced use-cases - see :ref:`lambda_overview` for details on when to use a Lambda store.

About this Tutorial
-------------------

In the spirit of keeping things simple, the code in this tutorial only
does a few small things:

1. Establishes a new (static) SimpleFeatureType
2. Prepares the Accumulo table and Kafka topic to write this type of data
3. Creates a few thousand example SimpleFeatures
4. Repeatedly updates these SimpleFeatures in the Lambda store through Kafka
5. Visualize the changing data in GeoServer
6. Persists the final SimpleFeatures to Accumulo
7. Uses GeoServer to visualize the data (optional)

Background
----------

`Apache Kafka <http://kafka.apache.org/>`__ is "publish-subscribe
messaging rethought as a distributed commit log."

In the context of GeoMesa, Kafka is a useful tool for working with
streams of geospatial data. The Lambda data store leverages a transient in-memory
cache of recent updates, powered by Kafka, combined with long-term persistence to
Accumulo. This allows for rapid data updates, alleviating the burden on Accumulo
from constant deletes and writes.

Prerequisites
-------------

Before you begin, you must have the following installed and configured:

-  `Java <http://java.oracle.com/>`__ JDK 1.8
-  Apache `Maven <http://maven.apache.org/>`__ |maven_version|
-  a GitHub client
-  a Kafka instance version |kafka_version|
-  an Accumulo |accumulo_version| instance
-  an Accumulo user that has both create-table and write permissions
-  the GeoMesa distributed runtime installed for your instance (see below)

Ensure your Kafka and Zookeeper instances are running. You can use
Kafka's `quickstart <http://kafka.apache.org/documentation.html#quickstart>`__
to get Kafka/Zookeeper instances up and running quickly.

Installing the GeoMesa Distributed Runtime
------------------------------------------

Follow the instructions under :ref:`install_accumulo_runtime` to install GeoMesa in your Accumulo instance.


Configure GeoServer (optional)
------------------------------

You can use GeoServer to access and visualize the data stored in GeoMesa. In order to use GeoServer,
download and install version |geoserver_version|. Then follow the instructions in :ref:`install_lambda_geoserver`
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
to set the appropriate versions for Accumulo, Hadoop, Kafka, Zookeeper, etc.

For ease of use, the project builds a bundled artifact that contains all the required
dependencies in a single JAR. To build, run:

.. code-block:: bash

    $ mvn clean install -pl geomesa-tutorials-accumulo/geomesa-tutorials-accumulo-lambda-quickstart -am

About this Tutorial
-------------------

The quick start operates by writing several thousand feature updates. The same feature identifier is used for
each update, so there will only be a single "live" feature at any one time. After
approximately 30 seconds, the updates stop and the feature is persisted to Accumulo.

The data used is from New York City taxi activity data published by the University
of Illinois. More information about the dataset is available `here <https://publish.illinois.edu/dbwork/open-data/>`__.

For this demo, only a single taxi is being tracked.

Running the Tutorial
--------------------

On the command line, run:

.. code-block:: bash

    $ java -cp geomesa-tutorials-accumulo/geomesa-tutorials-accumulo-lambda-quickstart/target/geomesa-tutorials-accumulo-lambda-quickstart-${geomesa.version}.jar \
        com.example.geomesa.lambda.LambdaQuickStart        \
        --lambda.accumulo.instance.id <instance>           \
        --lambda.accumulo.zookeepers <accumulo.zookeepers> \
        --lambda.accumulo.user <user>                      \
        --lambda.accumulo.password <password>              \
        --lambda.accumulo.catalog <table>                  \
        --lambda.kafka.brokers <brokers>                   \
        --lambda.kafka.zookeepers <kafka.zookeepers>       \
        --lambda.expiry 2s

where you provide the following arguments:

- ``<instance>`` the name of your Accumulo instance
- ``<accumulo.zookeepers>`` your Accumulo Zookeeper nodes, separated by commas
- ``<user>`` the name of an Accumulo user that has permissions to create, read and write tables
- ``<password>`` the password for the previously-mentioned Accumulo user
- ``<table>`` the name of the destination table that will accept these test records. This table should either not exist or should be empty
- ``<brokers>`` your Kafka broker instances, comma separated. For a local install, this would be ``localhost:9092``
- ``<kafka.zookeepers>`` your Kafka Zookeeper nodes, comma separated. For a local install, this would be ``localhost:2181``

.. warning::

    If you have set up the GeoMesa Accumulo distributed
    runtime to be isolated within a namespace (see
    :ref:`install_accumulo_runtime_namespace`) the value of ``<table>``
    should include the namespace (e.g. ``myNamespace.geomesa``).

Optionally, you can also specify that the quick start should delete its data upon completion. Use the
``--cleanup`` flag when you run to enable this behavior.

Once run, the quick start will create the Kafka topic, then pause and prompt you to register the layer in
GeoServer. If you do not want to use GeoServer, you can skip this step. Otherwise, follow the instructions in
the next section before returning here.

Once you continue, the tutorial should run for approximately thirty seconds. You should see the following output:

.. code-block:: none

    Loading datastore

    Creating schema: taxiId:String,dtg:Date,geom:Point

    Feature type created - register the layer 'tdrive-quickstart' in geoserver then hit <enter> to continue

    Generating test data

    Writing features to Kafka... refresh GeoServer layer preview to see changes
    Wrote 2202 features

    Waiting for expiry and persistence...
    Total features: 1, features persisted to Accumulo: 0
    Total features: 0, features persisted to Accumulo: 0
    Total features: 1, features persisted to Accumulo: 1

    Done

Visualize Data With GeoServer (optional)
----------------------------------------

You can use GeoServer to access and visualize the data stored in GeoMesa. In order to use GeoServer,
download and install version |geoserver_version|. Then follow the instructions in :ref:`install_lambda_geoserver`
to enable GeoMesa.

Register the GeoMesa Store with GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Log into GeoServer using your user and password credentials. Click
"Stores" and "Add new Store". Select the ``Kafka/Accumulo Lambda (GeoMesa)`` vector data
source, and fill in the required parameters.

Basic store info:

-  ``workspace`` this is dependent upon your GeoServer installation
-  ``data source name`` pick a sensible name, such as ``geomesa_quick_start``
-  ``description`` this is strictly decorative; ``GeoMesa quick start``

Connection parameters:

-  these are the same parameter values that you supplied on the
   command line when you ran the tutorial; they describe how to connect
   to the Kafka and Accumulo instances where your data reside

Click "Save", and GeoServer will search Zookeeper for any GeoMesa-managed feature types.

Publish the Layer
~~~~~~~~~~~~~~~~~

If you have already run the command to start the tutorial, then GeoServer should recognize the
``tdrive-quickstart`` feature type, and should present that as a layer that can be published. Click on the
"Publish" link. If not, then run the tutorial as described above in **Running the Tutorial**. When
the tutorial pauses, go to "Layers" and "Add new Layer". Select the GeoMesa Lambda store you just
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

Transient vs Persistent Features
--------------------------------

The layer preview will merge the results of features from Kafka with features from Accumulo. You may disable
results from one of the source by using the ``viewparams`` parameter:

.. code-block:: bash

    ...&viewparams=LAMBDA_QUERY_TRANSIENT:false
    ...&viewparams=LAMBDA_QUERY_PERSISTENT:false

While the quick start is running, all the features should be returned from the transient store (Kafka). After the quick
start finishes, all the feature should be returned from the persistent store (Accumulo). You can play with the
``viewparams`` to see the difference.

Looking at the Code
-------------------

The source code is meant to be accessible for this tutorial. The logic is contained in
the generic ``org.geomesa.example.quickstart.GeoMesaQuickStart`` in the ``geomesa-tutorials-common`` module,
and the Kafka/Accumulo-specific ``org.geomesa.example.lambda.LambdaQuickStart`` in the
``geomesa-tutorials-accumulo-lambda-quickstart`` module. Some relevant methods are:

-  ``createDataStore`` get a datastore instance from the input configuration
-  ``createSchema`` create the schema in the datastore, as a pre-requisite to writing data
-  ``writeFeatures`` overridden in the ``KafkaQuickStart`` to simultaneously write and read features from Kafka
-  ``queryFeatures`` not used in this tutorial
-  ``cleanup`` delete the sample data and dispose of the datastore instance

Looking at the source code, you can see that normal GeoTools ``FeatureWriters`` are used; feature persistence
is managed transparently for you.

The quickstart uses a small subset of taxi data. Code for parsing the data into GeoTools SimpleFeatures is
contained in ``org.geomesa.example.data.TDriveData``:

-  ``getSimpleFeatureType`` creates the ``SimpleFeatureType`` representing the data
-  ``getTestData`` parses an embedded CSV file to create ``SimpleFeature`` objects
-  ``getTestQueries`` not used in this tutorial

Re-Running the Quick Start
--------------------------

The quick start relies on not having any existing state when it runs. This can cause issues with older versions
of Kafka, which by default do not delete topics when requested. To re-run the quick start, first ensure that your Kafka
instance will delete topics by setting the configuration ``delete.topic.enable=true`` in your server properties.
Then use the Lamdba command-line tools (see :ref:`setting_up_lambda_commandline`) to remove the quick start schema:

.. code-block:: bash

    $ geomesa-lambda remove-schema -f tdrive-quickstart ...
