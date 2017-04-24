GeoMesa NiFi Bundle
===================

NiFi manages large batches and streams of files and data. GeoMesa-NiFi
allows you to ingest data into GeoMesa straight from NiFi by leveraging
custom processors.

Installation
------------

Install the Processors
~~~~~~~~~~~~~~~~~~~~~~

Pick a reasonable directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-nifi.git
    $ cd geomesa-nifi

To build, run

.. code-block:: bash

    $ mvn clean install

To install the GeoMesa processors you will need to copy the
geomesa-nifi-nar file from
``geomesa-nifi/geomesa-nifi-nar/target/geomesa-nifi-nar-$VERSION.nar``
into the ``lib/`` directory of your NiFi installation.

Install the SFTs and Converters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The GeoMesa processors need access to the SFTs and converters in order
to ingest data. There are two ways of providing these to the processors.
We can enter the SFT specification string and converter specification
string directly in a processor or we can provide these to the processors
by placing the SFTs and converters in a file named ``reference.conf``
and then putting that file on the classpath. This can be achieved by
wrapping this file in a JAR and placing it in the ``lib/`` directory of
the NiFi installation. For example you can wrap the ``reference.conf``
file in a JAR with this command.

.. code-block:: bash

    $ jar cvf data-formats.jar reference.conf

To validate everything is correct, run this command. Your results should
be similar.

.. code-block:: bash

    $ jar tvf data-formats.jar
         0 Mon Mar 20 18:18:36 EDT 2017 META-INF/
        69 Mon Mar 20 18:18:36 EDT 2017 META-INF/MANIFEST.MF
     28473 Mon Mar 20 14:49:54 EDT 2017 reference.conf

Processors
----------

This project contains four processors:

-  ``PutGeoMesa`` - Ingest data into GeoMesa Accumulo with a GeoMesa
   converter or from geoavro
-  ``PutGeoMesaKafka`` - Ingest data into GeoMesa Kafka with a GeoMesa
   converter or from geoavro
-  ``PutGeoTools`` - Ingest data into an arbitrary GeoTools Datastore
   based on parameters using a GeoMesa converter or avro
-  ``ConvertToGeoAvro`` - Use a GeoMesa converter to create geoavro

PutGeoMesa
~~~~~~~~~~

The ``PutGeoMesa`` processor is used for ingesting data into an Accumulo
backed GeoMesa datastore. To use this processor first add it to the
workspace and open the properties tab of its configuration. Descriptions
of the properties are given below:

+---------------------------------+------------------------------------------------------------------------------------------------------+
| Property                        | Description                                                                                          |
+=================================+======================================================================================================+
| Mode                            | Converter or Avro file ingest mode switch.                                                           |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| SftName                         | Name of the SFT on the classpath to use. This property overrides SftSpec.                            |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| ConverterName                   | Name of converter on the classpath to use. This property overrides ConverterSpec.                    |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| FeatureNameOverride             | Override the feature name on ingest.                                                                 |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| SftSpec                         | SFT specification String. Overwritten by SftName if SftName is valid.                                |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| ConverterSpec                   | Converter specification string. Overwritten by ConverterName if ConverterName is valid.              |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| instanceId                      | Accumulo instance ID                                                                                 |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| zookeepers                      | Comma separated list of zookeeper IPs or hostnames                                                   |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| user                            | Accumulo username with create-table and write permissions                                            |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| password                        | Accumulo password for given username                                                                 |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| visibilities                    | Accumulo scan visibilities                                                                           |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| tableName                       | Name of the table to write to. If using namespaces be sure to include that in the name.              |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| writeThreads                    | Number of threads to use when writing data to GeoMesa, has a linear effect on CPU and memory usage   |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| generateStats                   | Enable stats table generation                                                                        |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| useMock                         | Use a mock instance of Accumulo                                                                      |
+---------------------------------+------------------------------------------------------------------------------------------------------+
| GeoMesa Configuration Service   | Configuration service to use. More about this feature below.                                         |
+---------------------------------+------------------------------------------------------------------------------------------------------+

GeoMesa Configuration Service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``PutGeoMesa`` plugin supports `NiFi Controller
Services <http://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html>`__
to manage common configurations. This allows the user to specify a
single location to store the Accumulo connection parameters. This allows
you to add new PutGeoMesa processors without having to enter duplicate
data.

To add the ``GeomesaConfigControllerService`` access the
``Contoller Settings`` from NiFi global menu and navigate to the
``ControllerServices`` tab and click the ``+`` to add a new service.
Search for the ``GeomesaConfigControllerService`` and click add. Edit
the new service and enter the appropriate values for the properties
listed.

To use this feature, after configuring the service, select the
appropriate Geomesa Config Controller Service from the drop down of the
``GeoMesa Configuration Service`` property. When a controller service is
selected the ``zookeepers``, ``instanceId``, ``user``, ``password`` and
``tableName`` parameters are not required or used.

PutGeoMesaKafka
~~~~~~~~~~~~~~~

The ``PutGeoMesaKafka`` processor is used for ingesting data into a
Kafka backed GeoMesa datastore. This processor only supports Kafka 0.9
and newer. To use this processor first add it to the workspace and open
the properties tab of its configuration. Descriptions of the properties
are given below:

+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Property              | Description                                                                                                                                                                                   |
+=======================+===============================================================================================================================================================================================+
| Mode                  | Converter or Avro file ingest mode switch.                                                                                                                                                    |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| SftName               | Name of the SFT on the classpath to use. This property overrides SftSpec.                                                                                                                     |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ConverterName         | Name of converter on the classpath to use. This property overrides ConverterSpec.                                                                                                             |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| FeatureNameOverride   | Override the feature name on ingest.                                                                                                                                                          |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| SftSpec               | SFT specification String. Overwritten by SftName if SftName is valid.                                                                                                                         |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ConverterSpec         | Converter specification string. Overwritten by ConverterName if ConverterName is valid.                                                                                                       |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| brokers               | List of Kafka brokers                                                                                                                                                                         |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| zookeepers            | Comma separated list of zookeeper IPs or hostnames                                                                                                                                            |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| zkpath                | Zookeeper path to Kafka instance                                                                                                                                                              |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| namespace             | Kafka namespace to use                                                                                                                                                                        |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| partitions            | Number of partitions to use in Kafka topics                                                                                                                                                   |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| replication           | Replication factor to use in Kafka topics                                                                                                                                                     |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| isProducer            | Flag to mark if this is a producer                                                                                                                                                            |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| expirationPeriod      | Feature will be auto-dropped (expired) after this delay in milliseconds. Leave blank or use -1 to not drop features.                                                                          |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| cleanUpCache          | Run a thread to clean up the live feature cache if set to true. False by default. Use 'cleanUpCachePeriod' to configure the length of time between cache cleanups. Every second by default.   |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

PutGeoTools
~~~~~~~~~~~

The ``PutGeoTools`` processor is used for ingesting data into a GeoTools
compatible datastore. To use this processor first add it to the
workspace and open the properties tab of its configuration. Descriptions
of the properties are given below:

+-----------------------+-------------------------------------------------------------------------------------------+
| Property              | Description                                                                               |
+=======================+===========================================================================================+
| Mode                  | Converter or Avro file ingest mode switch.                                                |
+-----------------------+-------------------------------------------------------------------------------------------+
| SftName               | Name of the SFT on the classpath to use. This property overrides SftSpec.                 |
+-----------------------+-------------------------------------------------------------------------------------------+
| ConverterName         | Name of converter on the classpath to use. This property overrides ConverterSpec.         |
+-----------------------+-------------------------------------------------------------------------------------------+
| FeatureNameOverride   | Override the feature name on ingest.                                                      |
+-----------------------+-------------------------------------------------------------------------------------------+
| SftSpec               | SFT specification String. Overwritten by SftName if SftName is valid.                     |
+-----------------------+-------------------------------------------------------------------------------------------+
| ConverterSpec         | Converter specification string. Overwritten by ConverterName if ConverterName is valid.   |
+-----------------------+-------------------------------------------------------------------------------------------+
| DataStoreName         | Name of the datastore to ingest data into.                                                |
+-----------------------+-------------------------------------------------------------------------------------------+

This processor also accepts dynamic parameters that may be needed for
the specific datastore that you're trying to access.

ConvertToGeoAvro
~~~~~~~~~~~~~~~~

The ``ConvertToGeoAvro`` processor leverages GeoMesa's internal
converter framework to convert features into Avro and pass them along as
a flow to be used by other processors in NiFi. To use this processor
first add it to the workspace and open the properties tab of its
configuration. Descriptions of the properties are given below:

+-----------------------+-------------------------------------------------------------------------------------------+
| Property              | Description                                                                               |
+=======================+===========================================================================================+
| Mode                  | Converter or Avro file ingest mode switch.                                                |
+-----------------------+-------------------------------------------------------------------------------------------+
| SftName               | Name of the SFT on the classpath to use. This property override SftSpec.                  |
+-----------------------+-------------------------------------------------------------------------------------------+
| ConverterName         | Name of converter on the classpath to use. This property overrides ConverterSpec.         |
+-----------------------+-------------------------------------------------------------------------------------------+
| FeatureNameOverride   | Override the feature name on ingest.                                                      |
+-----------------------+-------------------------------------------------------------------------------------------+
| SftSpec               | SFT specification String. Overwritten by SftName if SftName is valid.                     |
+-----------------------+-------------------------------------------------------------------------------------------+
| ConverterSpec         | Converter specification string. Overwritten by ConverterName if ConverterName is valid.   |
+-----------------------+-------------------------------------------------------------------------------------------+
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
User
Guide <https://nifi.apache.org/docs/nifi-docs/html/user-guide.html>`__
