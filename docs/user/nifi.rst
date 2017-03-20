GeoMesa NiFi Plugins
====================

NiFi allows for managing large batches and streams of files and data.
GeoMesa-NiFi allows you to ingest data into GeoMesa strait from NiFi by
leveraging custom processors.

Installation
------------

Install the Processors
^^^^^^^^^^^^^^^^^^^^^^

Pick a reasonable directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-nifi.git
    $ cd geomesa-nifi

To build, run

.. code-block:: bash

    $ mvn clean install


To install the GoeMesa processors you will need to copy the geomesa-nifi-nar file from
``geomesa-nifi/geomesa-nifi-nar/target/geomesa-nifi-nar-$VERSION.nar`` into the ``lib/`` directory of your NiFi installation.

Install the SFTs and Converters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The GeoMesa processors need access to the SFTs and Converters in order to ingest data. We can provide these to the
processors by placing the SFTs and Converters in a file named ``reference.conf`` and then putting that file on the
classpath. This can be achieved by wrapping this file in a jar and placing it in the ``lib/`` directory of the NiFi
installation. For example you can wrap the ``reference.conf`` file in a jar with this command.

.. code-block:: bash

    $ jar cvf data-formats.jar reference.conf

To validate everything is correct, run this command. Your results should be similar.

.. code-block:: bash

    $ jar tvf data-formats.jar
         0 Mon Mar 20 18:18:36 EDT 2017 META-INF/
        69 Mon Mar 20 18:18:36 EDT 2017 META-INF/MANIFEST.MF
     28473 Mon Mar 20 14:49:54 EDT 2017 reference.conf

Processors
----------

This project contains four processors:

* PutGeoMesa       - Ingest data into GeoMesa Accumulo with a GeoMesa converter or from geoavro
* PutGeoMesaKafka  - Ingest data into GeoMesa Kafka with a GeoMesa converter or from geoavro
* PutGeoTools      - Ingest data into an arbitrary GeoTools Datastore based on parameters using a GeoMesa converter or avro
* ConvertToGeoAvro - Use a GeoMesa converter to create geoavro

PutGeoMesa
^^^^^^^^^^

The ``PutGeoMesa`` processor is used for ingesting data into an Accumulo backed GeoMesa datastore. To use this processor
first add it to the workspace and open the properties tab of it's configuration. Descriptions of the properties are
given below:

============================= ==========================================================================================
Property                      Description
============================= ==========================================================================================
Mode                          Converter or Avro file ingest mode switch.
SftName                       Name of the SFT on the classpath to use. This property override SftSpec.
ConverterName                 Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride           Override the feature name on ingest.
SftSpec                       SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec                 Converter specification string. Overwritten by ConverterName if ConverterName is valid.
instanceId                    Accumulo instance ID
zookeepers                    Comma separated list of zookeeper IPs or hostnames
user                          Accumulo username with create-table and write permissions
password                      Accumulo password for given username
visibilities                  Accumulo scan visibilities
tableName                     Name of the table to write to. If using namespaces but sure to include that in the name.
writeTreads                   Number of threads to use when writing data to GeoMesa, has a linear effect on CPU and
                              memory usage
generateStats                 Enable stats table generation
useMock                       Use a mock instance of Accumulo
GeoMesa Configuration Service Configuration service to use. More about this feature below.
============================= ==========================================================================================

GeoMesa Configuration Service
"""""""""""""""""""""""""""""

The ``PutGeoMesa`` plugin supports `NiFi Controller Services <http://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html>`__
to manage common configurations. This allows the user to specify a single location to store the Accumulo connection parameters.
This allows you to add new PutGeoMesa processors without having to enter duplicate datea.

To add the ``GeomesaConfigControllerService`` access the ``Contoller Settings`` from NiFi global menu and navigate to the
``ControllerServices`` tab and click the ``+`` to add a new service. Search for the ``GeomesaConfigControllerService``
and click add. Edit the new service and enter the appropriate values for the properties listed.

To use this feature, after configuring the service, select the appropriate Geomesa Config Controller Service from the drop down
of the ``GeoMesa Configuration Service`` property. When a controller service is selected the ``zookeepers``, ``instanceId``,
``user``, ``password`` and ``tableName`` parameters are not required or used.

PutGeoMesaKafka
^^^^^^^^^^^^^^^

The ``PutGeoMesaKafka`` processor is used for ingesting data into a Kafka backed GeoMesa datastore. To use this processor
first add it to the workspace and open the properties tab of it's configuration. Descriptions of the properties are
given below:

============================= ==========================================================================================
Property                      Description
============================= ==========================================================================================
Mode                          Converter or Avro file ingest mode switch.
SftName                       Name of the SFT on the classpath to use. This property override SftSpec.
ConverterName                 Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride           Override the feature name on ingest.
SftSpec                       SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec                 Converter specification string. Overwritten by ConverterName if ConverterName is valid.
brokers                       List of Kafka brokers
zookeepers                    Comma separated list of zookeeper IPs or hostnames
zkpath                        Zookeeper path to Kafka instance
namespace                     Kafka namespace to use
partitions                    Number of partitions to use in Kafka topics
replication                   Replication factor to use in Kafka topics
isProducer                    Flag to mark if this is a producer
expirationPeriod              Feature will be auto-dropped (expired) after this delay in milliseconds. Leave blank or
                              use -1 to not drop features.
cleanUpCache                  Run a thread to clean up the live feature cache if set to true. False by default. Use
                              'cleanUpCachePeriod' to configure the length of time between cache cleanups. Every second
                              by default.
============================= ==========================================================================================

PutGeoTools
^^^^^^^^^^^

The ``PutGeoTools`` processor is used for ingesting data into a GeoTools compatible datastore. To use this processor
first add it to the workspace and open the properties tab of it's configuration. Descriptions of the properties are
given below:

============================= ==========================================================================================
Property                      Description
============================= ==========================================================================================
Mode                          Converter or Avro file ingest mode switch.
SftName                       Name of the SFT on the classpath to use. This property override SftSpec.
ConverterName                 Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride           Override the feature name on ingest.
SftSpec                       SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec                 Converter specification string. Overwritten by ConverterName if ConverterName is valid.
DataStoreName                 Name of the datastore to ingest data into.
============================= ==========================================================================================

This processor also accepts dynamic parameters that my be needed for the specific datastore that you're trying to access.

ConvertToGeoAvro
^^^^^^^^^^^^^^^^

The ``ConvertToGeoAvro`` processor leverages GeoMesa's internal converter framework to convert features into Avro and pass them
along as a flow to be used by other processors in NiFi. To use this processor first add it to the workspace and open
the properties tab of it's configuration. Descriptions of the properties are given below:

============================= ==========================================================================================
Property                      Description
============================= ==========================================================================================
Mode                          Converter or Avro file ingest mode switch.
SftName                       Name of the SFT on the classpath to use. This property override SftSpec.
ConverterName                 Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride           Override the feature name on ingest.
SftSpec                       SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec                 Converter specification string. Overwritten by ConverterName if ConverterName is valid.
OutputFormat                  Only Avro is supported at this time.
============================= ==========================================================================================

Reference
---------

For more information on setting up or using NiFi see the `Apache NiFi User Guide <https://nifi.apache.org/docs/nifi-docs/html/user-guide.html>`__
