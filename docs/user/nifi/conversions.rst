Conversion Processors
---------------------

ConvertToGeoAvro
~~~~~~~~~~~~~~~~

The ``ConvertToGeoAvro`` processor leverages GeoMesa's internal
converter framework to convert files into Avro and pass them along as
a flow to be used by other processors in NiFi. To use this processor
first add it to the workspace and open the properties tab of its
configuration.

+-----------------------+-------------------------------------------------------------------------------------------+
| Property              | Description                                                                               |
+=======================+===========================================================================================+
| OutputFormat          | Only Avro is supported at this time.                                                      |
+-----------------------+-------------------------------------------------------------------------------------------+

GeoAvro Record Writer
~~~~~~~~~~~~~~~~~~~~~

GeoMesa also provides a record writer which will write out data in the GeoAvro format.
This component is configured as a service and then configured on a Processor which requires a record writer.
The configuration mirrors that for the :ref:`nifi_record_input_configuration`.
