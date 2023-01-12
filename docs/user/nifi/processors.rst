Processors
----------

GeoMesa NiFi provides several processors:

+-----------------------------------+-----------------------------------------------------------------+
| Processor                         | Description                                                     |
+===================================+=================================================================+
| ``PutGeoMesa``                    | Ingest data into a GeoMesa data store using a GeoMesa converter |
+-----------------------------------+-----------------------------------------------------------------+
| ``PutGeoMesaRecord``              | Ingest data into a GeoMesa data store using the NiFi record API |
+-----------------------------------+-----------------------------------------------------------------+
| ``AvroToPutGeoMesa``              | Ingest GeoAvro files into a GeoMesa data store                  |
+-----------------------------------+-----------------------------------------------------------------+
| ``UpdateGeoMesaRecord``           | Update existing records in a GeoMesa data store using the NiFi  |
|                                   | record API                                                      |
+-----------------------------------+-----------------------------------------------------------------+
| ``GetGeoMesaKafkaRecord``         | Read GeoMesa Kafka messages and output them as NiFi records     |
+-----------------------------------+-----------------------------------------------------------------+
| ``ConvertToGeoFile``              | Create files in a variety of geometry-enabled formats using a   |
|                                   | GeoMesa converter                                               |
+-----------------------------------+-----------------------------------------------------------------+

Each processor (with the exception of ``ConvertToGeoFile``) needs to be configured with a ``DataStoreService``,
which will connect to the GeoMesa back-end data source (for example, HBase or Kafka). See
:ref:`nifi_datstore_services` for the available services.

Records, Converters, and Avro
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

GeoMesa provides three different ingest processors. They all write to the same data stores, but
they vary in how the input data is converted into GeoTools ``SimpleFeatures`` (which are necessary for ingest).

The ``PutGeoMesa`` processor uses the :ref:`converters` framework to define ``SimpleFeatureTypes`` and the mapping
from input files to ``SimpleFeatures``. Converters can be re-used in the GeoMesa command-line tools and other
non-NiFi projects. See :doc:`/user/nifi/converters` for details.

The ``PutGeoMesaRecord`` processor uses the NiFi records API to define the input schema using a NiFi ``RecordReader``.
Through ``RecordReaders``, ``SimpleFeatureTypes`` can be managed in a centralized schema registry. Similarly, records
can be manipulated using standard NiFi processors before being passed to the GeoMesa processor. The use of standard
NiFi APIs greatly reduces the amount of GeoMesa-specific configuration required. See :doc:`/user/nifi/records`
for details.

Finally, the ``AvroToPutGeoMesa`` processor will ingest GeoMesa-specific GeoAvro files without any configuration.
GeoAvro is a special Avro file that has ``SimpleFeatureType`` metadata included. It can be produced using the
GeoMesa command-line tools export in ``avro`` format, the ``ConvertToGeoFile`` processor, the
``GeoAvroRecordSetWriterFactory`` record writer factory, or directly through an instance of
``org.locationtech.geomesa.features.avro.io.AvroDataFileWriter``. GeoAvro is particularly useful because it is
self-describing. See :doc:`/user/nifi/avro` for details.

Common Configuration
~~~~~~~~~~~~~~~~~~~~

All types of input processors have some common configuration parameters for controlling data store writes:

+-------------------------------+-----------------------------------------------------------------------------------------+
| Property                      | Description                                                                             |
+===============================+=========================================================================================+
| ``DataStore Service``         | Controller service to manage the GeoMesa data store being used                          |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``Write Mode``                | Use an appending writer (for new features) or a modifying writer (to update existing    |
|                               | features)                                                                               |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``Identifying Attribute``     | When using a modifying writer, the attribute used to uniquely identify the feature.     |
|                               | If not specified, will use the feature ID                                               |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``Schema Compatibility``      | Controls how differences between the configured schema and the existing schema in the   |
|                               | data store (if any) will be handled.                                                    |
|                               |                                                                                         |
|                               | * ``Existing`` will use the existing schema and drop any additional fields in the       |
|                               |   configured schema.                                                                    |
|                               | * ``Update`` will update the existing schema to match the configured schema.            |
|                               | * ``Exact`` requires the configured schema to  match the existing schema.               |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``FeatureWriterCaching``      | Enable caching of feature writers between flow files, useful if flow files have a       |
|                               | small number of records (see below)                                                     |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``FeatureWriterCacheTimeout`` | How often feature writers will be flushed to the data store, if caching is enabled      |
+-------------------------------+-----------------------------------------------------------------------------------------+

Feature Writer Caching
^^^^^^^^^^^^^^^^^^^^^^

Feature writer caching can be used to improve the throughput of processing many small flow files. Instead of a new
feature writer being created for each flow file, writers are cached and re-used between operations. If a writer is
idle for the configured timeout, then it will be flushed to the data store and closed.

Note that if feature writer caching is enabled, features that are processed may not show up in the data store
immediately. In addition, any features that have been processed but not flushed may be lost if NiFi shuts down
unexpectedly. To ensure data is properly flushed, stop the processor before shutting down NiFi.

Alternatively, NiFi's built-in ``MergeContent`` processor can be used to batch up small files.
