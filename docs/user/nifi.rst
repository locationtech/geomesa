.. _nifi_bundle:

GeoMesa NiFi Bundle
===================

NiFi manages large batches and streams of files and data. GeoMesa-NiFi
allows you to ingest data into GeoMesa straight from NiFi by leveraging
custom processors.

Installation
------------

Get the Processors
~~~~~~~~~~~~~~~~~~

The GeoMesa NiFi processors are available for download from `GitHub <https://github.com/geomesa/geomesa-nifi/releases>`__.

Alternatively, you may build the processors from source. First, clone the project from GitHub. Pick a reasonable
directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-nifi.git
    $ cd geomesa-nifi

To build the project, run:

.. code-block:: bash

    $ mvn clean install

The nar contains bundled dependencies. To change the dependency versions, modify the version properties
(``<hbase.version>``, etc) in the ``pom.xml`` before building.

Install the Processors
~~~~~~~~~~~~~~~~~~~~~~

To install the GeoMesa processors you will need to copy the nar files into the ``lib`` directory of your
NiFi installation. There currently two common nar files, and seven datastore-specific nar files.

Common nar files:

* ``geomesa-datastore-services-api-nar-$VERSION.nar``
* ``geomesa-datastore-services-nar-$VERSION.nar``

Datastore nar files:

* ``geomesa-kafka-nar-$VERSION.nar``
* ``geomesa-hbase1-nar-$VERSION.nar``
* ``geomesa-hbase2-nar-$VERSION.nar``
* ``geomesa-redis-nar-$VERSION.nar``
* ``geomesa-accumulo1-nar-$VERSION.nar``
* ``geomesa-accumulo2-nar-$VERSION.nar``
* ``geomesa-fs-nar-$VERSION.nar``

The common nar files are required for all datastores. The datastore-specific nars can be installed as needed.

.. note::

  There are two HBase and Accumulo nars that correspond to HBase/Accumulo 1.x and HBase/Accumulo 2.x, respectively.
  Be sure to choose the appropriate nar for your database version.

If you downloaded the nars from GitHub:

.. code-block:: bash

    $ export NARS="geomesa-hbase2-nar geomesa-datastore-services-api-nar geomesa-datastore-services-nar"
    $ for nar in $NARS; do wget "https://github.com/geomesa/geomesa-nifi/releases/download/geomesa-nifi-$VERSION/$nar-$VERSION.nar"; done
    $ mv *.nar $NIFI_HOME/lib/

Or, to install the nars after building from source:

.. code-block:: bash

    $ export NARS="geomesa-hbase2-nar geomesa-datastore-services-api-nar geomesa-datastore-services-nar"
    $ for nar in $NARS; do find . -name $nar-$VERSION.nar -exec cp {} $NIFI_HOME/lib/ \;; done

Processors
----------

GeoMesa NiFi contains several processors:

+--------------------------+----------------------------------------------------------------------------+
| Processor                | Description                                                                |
+==========================+============================================================================+
| ``PutGeoMesaAccumulo``   | Ingest data into a GeoMesa Accumulo datastore with a GeoMesa converter     |
+--------------------------+----------------------------------------------------------------------------+
| ``PutGeoMesaHBase``      | Ingest data into a GeoMesa HBase datastore with a GeoMesa converter        |
+--------------------------+----------------------------------------------------------------------------+
| ``PutGeoMesaFileSystem`` | Ingest data into a GeoMesa File System datastore with a GeoMesa converter  |
+--------------------------+----------------------------------------------------------------------------+
| ``PutGeoMesaKafka``      | Ingest data into a GeoMesa Kafka datastore with a GeoMesa converter        |
+--------------------------+----------------------------------------------------------------------------+
| ``PutGeoMesaRedis``      | Ingest data into a GeoMesa Redis datastore with a GeoMesa converter        |
+--------------------------+----------------------------------------------------------------------------+
| ``PutGeoTools``          | Ingest data into an arbitrary GeoTools datastore using a GeoMesa converter |
+--------------------------+----------------------------------------------------------------------------+
| ``AvroToPut*``           | Ingest self-defining GeoAvro instead of configuring a converter            |
+--------------------------+----------------------------------------------------------------------------+
| ``ConvertToGeoAvro``     | Use a GeoMesa converter to create GeoAvro                                  |
+--------------------------+----------------------------------------------------------------------------+

Input Configuration
~~~~~~~~~~~~~~~~~~~

Most of the processors accept similar configuration parameters for specifying the input source. Each
datastore-specific processor also has additional parameters for connecting to the datastore, detailed in the
following sections.

+-------------------------------+-----------------------------------------------------------------------------------------+
| Property                      | Description                                                                             |
+===============================+=========================================================================================+
| ``SftName``                   | Name of the SFT on the classpath to use. This property overrides SftSpec.               |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConverterName``             | Name of converter on the classpath to use. This property overrides ConverterSpec.       |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``FeatureNameOverride``       | Override the feature name on ingest.                                                    |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``SftSpec``                   | SFT specification String. Overwritten by SftName if SftName is valid.                   |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConverterSpec``             | Converter specification string. Overwritten by ConverterName if ConverterName is valid. |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConverterErrorMode``        | Override the converter error mode (``skip-bad-records`` or ``raise-errors``)            |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ExtraClasspaths``           | Additional resources to add to the classpath, usually converter definitions.            |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``BatchSize``                 | The number of flow files that will be processed in a single batch                       |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``FeatureWriterCaching``      | Enable caching of feature writers between flow files, useful if flow files have a       |
|                               | small number of records (see below)                                                     |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``FeatureWriterCacheTimeout`` | How often feature writers will be flushed to the data store, if caching is enabled      |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConvertFlowFileAttributes`` | Expose flow file attributes to the converter framework, referenced by name              |
+-------------------------------+-----------------------------------------------------------------------------------------+

Defining SimpleFeatureTypes and Converters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The GeoMesa NiFi processors package a set of predefined SimpleFeatureType schema definitions and GeoMesa
converter definitions for popular data sources such as Twitter, GDelt and OpenStreetMaps.

The full list of provided sources can be found in :ref:`prepackaged_converters`.

For custom data sources, there are two ways of providing custom SFTs and converters:

Providing SimpleFeatureTypes and Converters on the Classpath
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

To bundle configuration in a JAR file simply place your config in a file named ``reference.conf`` and place it **at
the root level** of a JAR file:

.. code-block:: bash

    $ jar cvf data-formats.jar reference.conf

You can verify your JAR was built properly:

.. code-block:: bash

    $ jar tvf data-formats.jar
         0 Mon Mar 20 18:18:36 EDT 2017 META-INF/
        69 Mon Mar 20 18:18:36 EDT 2017 META-INF/MANIFEST.MF
     28473 Mon Mar 20 14:49:54 EDT 2017 reference.conf

Use the ``ExtraClasspaths`` property to point your processor to the JAR file. The property takes a list of
comma-delimited resources. Once set, the ``SftName`` and/or ``ConverterName`` properties will update with the
name of your converters. You will need to close the configuration panel and re-open it in order for the
properties to update.

Defining SimpleFeatureTypes and Converters via the UI
+++++++++++++++++++++++++++++++++++++++++++++++++++++

You may also provide SimpleFeatureTypes and Converters directly in the Processor configuration via the NiFi UI.
Simply paste your TypeSafe configuration into the ``SftSpec`` and ``ConverterSpec`` property fields.

Defining SimpleFeatureTypes and Converters via Flow File Attributes
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

You may also override the Processor configuration fields with flow file attributes. The following attributes
are available:

* ``geomesa.sft.name`` corresponds to the Processor configuration ``FeatureNameOverride``
* ``geomesa.sft.spec`` corresponds to the Processor configuration ``SftSpec``
* ``geomesa.converter`` corresponds to the Processor configuration ``ConverterSpec``

.. warning::

    Configuration via flow file attributes should be used with care, as any misconfigurations may multiply.
    For example, setting ``geomesa.sft.name`` to a non-recurring value could end up creating a new schema for each
    flow file, potentially crashing your database by creating too many tables.

Feature Writer Caching
^^^^^^^^^^^^^^^^^^^^^^

Feature writer caching can be used to improve the throughput of processing many small flow files. Instead of a new
feature writer being created for each flow file, writers are cached and re-used between operations. If a writer is
idle for the configured timeout, then it will be flushed to the data store and closed.

Note that if feature writer caching is enabled, features that are processed may not show up in the data store
immediately. In addition, any features that have been processed but not flushed may be lost if NiFi shuts down
unexpectedly. To ensure data is properly flushed, stop the processor before shutting down NiFi.

Alternatively, NiFi's built-in ``MergeContent`` processor can be used to batch up small files.

PutGeoMesaAccumulo
~~~~~~~~~~~~~~~~~~

The ``PutGeoMesaAccumulo`` processor is used for ingesting data into an Accumulo-backed GeoMesa datastore. To use
this processor, first add it to the workspace and open the properties tab of its configuration. For a description
of the connection properties, see :ref:`accumulo_parameters`.

GeoMesa Configuration Service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``PutGeoMesaAccumulo`` plugin supports
`NiFi Controller Services <https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#Controller_Services>`__
to manage common configurations. This allows the user to specify a single location to store the Accumulo connection
parameters. This allows you to add new processors without having to enter duplicate data.

To add the ``AccumuloDataStoreConfigControllerService`` access the ``Contoller Settings`` from NiFi global menu and
navigate to the ``ControllerServices`` tab and click the ``+`` to add a new service. Search for the
``AccumuloDataStoreConfigControllerService`` and click add. Edit the new service and enter the appropriate values
for the properties listed.

After configuring the service, select the appropriate service in the ``GeoMesa Configuration Service`` property
of your processor. When a controller service is selected the ``accumulo.zookeepers``, ``accumulo.instance.id``,
``accumulo.user``, ``accumulo.password`` and ``accumulo.catalog`` parameters are not required or used.

PutGeoMesaHBase
~~~~~~~~~~~~~~~

The ``PutGeoMesaHBase`` processor is used for ingesting data into an HBase-backed GeoMesa datastore. To use
this processor, first add it to the workspace and open the properties tab of its configuration. For a description
of the connection properties, see :ref:`hbase_parameters`.

PutGeoMesaFileSystem
~~~~~~~~~~~~~~~~~~~~

The ``PutGeoMesaFileSystem`` processor is used for ingesting data into a file system-backed GeoMesa datastore. To use
this processor, first add it to the workspace and open the properties tab of its configuration. For a description
of the connection properties, see :ref:`fsds_parameters`.

PutGeoMesaKafka
~~~~~~~~~~~~~~~

The ``PutGeoMesaKafka`` processor is used for ingesting data into a
Kafka-backed GeoMesa datastore. This processor supports Kafka 0.9
and newer. To use this processor first add it to the workspace and open
the properties tab of its configuration. For a description
of the connection properties, see :ref:`kafka_parameters`.

PutGeoMesaRedis
~~~~~~~~~~~~~~~

The ``PutGeoMesaRedis`` processor is used for ingesting data into a Redis-backed GeoMesa datastore. To use this
processor first add it to the workspace and open the properties tab of its configuration. For a description
of the connection properties, see :ref:`redis_parameters`.

PutGeoTools
~~~~~~~~~~~

The ``PutGeoTools`` processor is used for ingesting data into any GeoTools
compatible datastore. To use this processor first add it to the
workspace and open the properties tab of its configuration.

+-----------------------+-------------------------------------------------------------------------------------------+
| Property              | Description                                                                               |
+=======================+===========================================================================================+
| DataStoreName         | Name of the datastore to ingest data into.                                                |
+-----------------------+-------------------------------------------------------------------------------------------+

This processor also accepts dynamic parameters that may be needed for
the specific datastore that you're trying to access.

AvroToPut*
~~~~~~~~~~

Each of the Put processors provided by GeoMesa has a corresponding AvroToPut processor. The Avro processors
do not require a GeoMesa converter or SimpleFeatureType, as they only accept self-describing GeoAvro.
GeoAvro can be generated through the GeoMesa command-line tools ``export`` functionality, the ConvertToGeoAvro
processor, or directly through an instance of ``org.locationtech.geomesa.features.avro.AvroDataFileWriter``.

ConvertToGeoAvro
~~~~~~~~~~~~~~~~

The ``ConvertToGeoAvro`` processor leverages GeoMesa's internal
converter framework to convert features into Avro and pass them along as
a flow to be used by other processors in NiFi. To use this processor
first add it to the workspace and open the properties tab of its
configuration.

+-----------------------+-------------------------------------------------------------------------------------------+
| Property              | Description                                                                               |
+=======================+===========================================================================================+
| OutputFormat          | Only Avro is supported at this time.                                                      |
+-----------------------+-------------------------------------------------------------------------------------------+

NiFi User Notes
---------------

NiFi allows you to ingest data into GeoMesa from every source GeoMesa
supports and more. Some of these sources can be tricky to setup and
configure. Here we detail some of the problems we've encountered and how
to resolve them.

GetHDFS Processor with Azure Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to use the `Hadoop Azure
Support <https://hadoop.apache.org/docs/current/hadoop-azure/index.html>`__
to access Azure Blob Storage using HDFS. You can leverage this
capability to have the GetHDFS processor pull data directly from the
Azure Blob store. However, due to how the GetHDFS processor was written,
the ``fs.defaultFS`` configuration property is always used when
accessing ``wasb://`` URIs. This means that the ``wasb://`` container
you want the GetHDFS processor to connect to must be hard coded in the
HDFS ``core-site.xml`` config. This causes two problems. Firstly, it
implies that we can only connect to one container in one account on
Azure. Secondly, it causes problems when using NiFi on a server that is
also running GeoMesa-Accumulo as the ``fs.defaultFS`` property needs to
be set to the proper HDFS master NameNode.

There are two ways to circumvent this problem. You can maintain a
``core-site.xml`` file for each container you want to access but this is
not easily scalable or maintainable in the long run. The better option
is to leave the default ``fs.defaultFS`` value in the HDFS
``core-site.xml`` file. We can then leverage the
``Hadoop Configuration Resources`` property in the GetHDFS processor.
Normally you would set the ``Hadoop Configuration Resources`` property
to the location of the ``core-site.xml`` and the ``hdfs-site.xml``
files. Instead we are going to create an additional file and include it
last in the path so that it overwrites the value of the ``fs.defaultFS``
when the configuration object is built. To do this simply create a new
xml file with an appropriate name (we suggest the name of the
container). Insert the ``fs.defaultFS`` property into the file and set
the value.

.. code-block:: xml

    <configuration>
        <property>
            <name>fs.defaultFS</name>
            <value>wasb://container@accountName.blob.core.windows.net/</value>
        </property>
    </configuration>

Reference
---------

For more information on setting up or using NiFi see the `Apache NiFi
User Guide <https://nifi.apache.org/docs/nifi-docs/html/user-guide.html>`__
