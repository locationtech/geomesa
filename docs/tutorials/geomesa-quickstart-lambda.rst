GeoMesa Lambda Quick Start
==========================

This tutorial can get you started with the GeoMesa Lambda data store. Note that the Lambda data store
is for advanced use-cases - see :ref:`lambda_overview` for details on when to use a Lambda store.

In the spirit of keeping things simple, the code in this tutorial only does a few small things:

1. Establishes a new (static) SimpleFeatureType
2. Prepares the Accumulo table and Kafka topic to store this type of data
3. Creates a thousand example SimpleFeatures
4. Repeatedly updates these SimpleFeatures in the Lambda store through Kafka
5. Persists the final SimpleFeatures to Accumulo

The only dynamic element in the tutorial is the Accumulo and Kafka connection;
it needs to be provided on the command-line when running the code.

Prerequisites
-------------

Before you begin, you must have the following:

-  an instance of Accumulo |accumulo_version| running on Hadoop |hadoop_version|,
-  an Accumulo user that has both create-table and write permissions,
-  the GeoMesa Accumulo distributed runtime installed for your Accumulo instance (see :ref:`install_accumulo_runtime` ),
-  an instance of Kafka 0.9.0.1,
-  optionally, a GeoServer instance with the Lambda data store installed (see :ref:`install_lambda_geoserver`),
-  a local copy of `Java JDK 8`_,
-  Apache `Maven <http://maven.apache.org/>`__ installed, and
-  a GitHub client installed.

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

    $ mvn clean install -pl geomesa-quickstart-lambda

.. note::

    Ensure that the version of Accumulo, Hadoop, Kafka etc in
    the root ``pom.xml`` match your environment.

.. note::

    Depending on the version, you may also need to build
    GeoMesa locally. Instructions can be found
    `here <https://github.com/locationtech/geomesa/>`__.

About this Tutorial
-------------------

The QuickStart operates by inserting 1000 features, and then updating them every 200 milliseconds. After
approximately 30 seconds, the updates stop and the features are persisted to Accumulo.

Run the Tutorial
----------------

On the command-line, run:

.. code-block:: bash

    $ java -cp geomesa-quickstart-lambda/target/geomesa-quickstart-lambda-${geomesa.version}.jar \
      com.example.geomesa.lambda.LambdaQuickStart \
      --brokers <brokers>                         \
      --instance <instance>                       \
      --zookeepers <zookeepers>                   \
      --user <user>                               \
      --password <password>                       \
      --catalog <table>

where you provide the following arguments:

-  ``<brokers>`` the host:port for your Kafka brokers
-  ``<instance>`` the name of your Accumulo instance
-  ``<zookeepers>`` your Zookeeper nodes, separated by commas
-  ``<user>`` the name of an Accumulo user that has permissions to create, read and write tables
-  ``<password>`` the password for the previously-mentioned Accumulo user
-  ``<table>`` the name of the destination table that will accept these test records; this table should either not exist or should be empty

.. warning::

    If you have set up the GeoMesa Accumulo distributed
    runtime to be isolated within a namespace (see
    :ref:`install_accumulo_runtime_namespace`) the value of ``<table>``
    should include the namespace (e.g. ``myNamespace.geomesa``).

Once you run the quick start, it will prompt you to load the layer in geoserver. Using the same connection
parameters you used for the quick start, register a new data store according to :ref:`create_lambda_ds_geoserver`.
After saving the store, you should be able to publish the ``lambda-quick-start`` layer. Open the layer preview for
the layer, then proceed with the quick start run.

As the quick start runs, you should be able to refresh the layer preview page and see the features moving across
the map. After approximately 30 seconds, the updates will stop, and the features will be persisted to Accumulo.

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

Looking at the source code, you can see that normal GeoTools ``FeatureWriters`` are used; feature persistence
is managed transparently for you.

Re-Running the Quick Start
--------------------------

The quick start relies on not having any existing state when it runs. This can cause issues with Kafka, which
by default does not delete topics when requested. To re-run the quick start, first ensure that your Kafka
instance will delete topics by setting the configuration ``delete.topic.enable=true`` in your server properties.
Then use the Lamdba command-line tools (see :ref:`setting_up_lambda_commandline`) to remove the quick start schema:

.. code-block:: bash

    $ geomesa-lambda remove-schema -f lambda-quick-start ...
