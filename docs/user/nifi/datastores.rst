DataStore Processors
--------------------

HBase
~~~~~

The ``PutGeoMesaHBase``, ``PutGeoMesaHBaseRecord``, ``UpdateGeoMesaHBaseRecord`` and ``AvroToPutGeoMesaHBase``
processors are used for ingesting data into an HBase-backed GeoMesa datastore. To use these processors, first
add one to the workspace and open the properties tab of its configuration. For a description of the connection
properties, see :ref:`hbase_parameters`.

Accumulo
~~~~~~~~

The ``PutGeoMesaAccumulo``, ``PutGeoMesaAccumuloRecord``, ``UpdateGeoMesaAccumuloRecord`` and
``AvroToPutGeoMesaAccumulo`` processors are used for ingesting data into an Accumulo-backed GeoMesa datastore.
To use these processors, first add one to the workspace and open the properties tab of its configuration.
For a description of the connection properties, see :ref:`accumulo_parameters`.

GeoMesa Configuration Service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Accumulo processors support
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

FileSystem
~~~~~~~~~~

The ``PutGeoMesaFileSystem``, ``PutGeoMesaFileSystemRecord``, ``UpdateGeoMesaFileSystemRecord`` and
``AvroToPutGeoMesaFileSystem`` processors are used for ingesting data into an file-system-backed GeoMesa datastore.
To use these processors, first add one to the workspace and open the properties tab of its configuration. For a
description of the connection properties, see :ref:`fsds_parameters`.

Kafka
~~~~~

The ``PutGeoMesaKafka``, ``PutGeoMesaKafkaRecord`` and ``AvroToPutGeoMesaKafka`` processors are used for
ingesting data into a Kafka-backed GeoMesa datastore. To use these processors, first add one to the workspace
and open the properties tab of its configuration. For a description of the connection properties, see
:ref:`kafka_parameters`.

Redis
~~~~~

The ``PutGeoMesaRedis``, ``PutGeoMesaRedisRecord``, ``UpdateGeoMesaRedisRecord`` and ``AvroToPutGeoMesaRedis``
processors are used for ingesting data into an Redis-backed GeoMesa datastore. To use these processors, first
add one to the workspace and open the properties tab of its configuration. For a description of the connection
properties, see :ref:`redis_parameters`.

GeoTools
~~~~~~~~

The ``PutGeoTools``, ``PutGeoToolsRecord``, ``UpdateGeoToolsRecord`` and ``AvroToPutGeoTools``
processors are used for ingesting data into any GeoTools compatible datastore. To use these processors, first
add one to the workspace and open the properties tab of its configuration:

+-----------------------+-------------------------------------------------------------------------------------------+
| Property              | Description                                                                               |
+=======================+===========================================================================================+
| DataStoreName         | Name of the datastore to ingest data into.                                                |
+-----------------------+-------------------------------------------------------------------------------------------+

This processor also accepts dynamic parameters that may be needed for the specific datastore that you're
trying to access. Additional data store dependencies may be required, which can be added through the
``ExtraClasspaths`` property.
