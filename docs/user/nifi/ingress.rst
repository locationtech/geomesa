Ingress Processors
------------------

GetGeoMesaKafkaRecord
~~~~~~~~~~~~~~~~~~~~~

The ``GetGeoMesaKafkaRecord`` processor provides the ability to read messages written by the GeoMesa Kafka data store
and output them as NiFi records for further processing.

.. warning::

  The ``GetGeoMesaKafkaRecord`` has not been tested with multiple processor threads, and may not work as expected.
  ``kafka.consumer.count`` can be used to configure the number of consumer threads in a given processor.

+-------------------------------+----------------------------------------------------------------------------------------+
| Property                      | Description                                                                            |
+===============================+========================================================================================+
| kafka.brokers                 | The Kafka brokers, in the form of ``host1:port1,host2:port2``                          |
+-------------------------------+----------------------------------------------------------------------------------------+
| kafka.zookeepers              | The Kafka zookeepers, in the form of ``host1:port1,host2:port2``                       |
+-------------------------------+----------------------------------------------------------------------------------------+
| kafka.zk.path                 | The zookeeper discoverable path, used to namespace schemas                             |
+-------------------------------+----------------------------------------------------------------------------------------+
| Type Name                     | The simple feature type name to read                                                   |
+-------------------------------+----------------------------------------------------------------------------------------+
| Kafka Group ID                | The Kafka consumer group ID, used to track messages read                               |
+-------------------------------+----------------------------------------------------------------------------------------+
| Record Writer                 | The NiFi record writer service used to serialize records                               |
+-------------------------------+----------------------------------------------------------------------------------------+
| Geometry Serialization Format | The format to use for serializing geometries, either text or binary                    |
+-------------------------------+----------------------------------------------------------------------------------------+
| Include Visibilities          | Include a column with visibility expressions for each row                              |
+-------------------------------+----------------------------------------------------------------------------------------+
| Include User Data             | Include a column with user data from the SimpleFeature, serialized as JSON             |
+-------------------------------+----------------------------------------------------------------------------------------+
| Record Maximum Batch Size     | The maximum number of records to output in a single flow file                          |
+-------------------------------+----------------------------------------------------------------------------------------+
| Record Minimum Batch Size     | The minimum number of records to output in a single flow file                          |
+-------------------------------+----------------------------------------------------------------------------------------+
| Record Max Latency            | The maximum delay between receiving a message and writing it out as a flow file.       |
|                               | Takes precedence over minimum batch size if both are set                               |
+-------------------------------+----------------------------------------------------------------------------------------+
| Consumer Poll Timeout         | The amount of time to wait for new records before writing out a flow file,             |
|                               | subject to batch size restrictions                                                     |
+-------------------------------+----------------------------------------------------------------------------------------+
| Kafka Initial Offset          | The initial offset to use when reading messages from a new topic                       |
+-------------------------------+----------------------------------------------------------------------------------------+
| kafka.consumer.count          | The number of consumers (threads) to use for reading messages                          |
+-------------------------------+----------------------------------------------------------------------------------------+
| kafka.consumer.config         | `Configuration options <https://kafka.apache.org/documentation.html#consumerconfigs>`_ |
|                               | for the kafka consumer, in Java properties format                                      |
+-------------------------------+----------------------------------------------------------------------------------------+

Note that any processors with the same Kafka Group ID will split messages between the processors, as per standard
Kafka consumer group behavior. Generally this is not desirable, and a unique group ID should be used for each
processor.

Attributes
^^^^^^^^^^

The ``GetGeoMesaKafkaRecord`` will set the following NiFi expression attributes, for use in the configured record writer:

+-------------------------------+---------------------------------------------------------------------------------------+
| Attribute                     | Description                                                                           |
+===============================+=======================================================================================+
| ``geomesa.id.col``            | The name of the Feature ID column in the output record                                |
+-------------------------------+---------------------------------------------------------------------------------------+
| ``geomesa.geometry.cols``     | The name and types of any geometry columns in the output record, comma-separated      |
+-------------------------------+---------------------------------------------------------------------------------------+
| ``geomesa.default.dtg.col``   | The name of the default date column in the output record                              |
+-------------------------------+---------------------------------------------------------------------------------------+
| ``geomesa.json.cols``         | The name of any JSON-type string columns in the output record, comma-separated        |
+-------------------------------+---------------------------------------------------------------------------------------+
| ``geomesa.visibilities.col``  | The name of the visibilities column in the output record                              |
+-------------------------------+---------------------------------------------------------------------------------------+

These properties correspond to the default configuration of the GeoMesa :ref:`nifi_record_input_configuration`,
so generally no additional configuration is needed to read from Kafka and write to another data store.
