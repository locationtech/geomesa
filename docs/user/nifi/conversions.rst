Conversion Processors
---------------------

ConvertToGeoFile
~~~~~~~~~~~~~~~~

The ``ConvertToGeoFile`` processor uses GeoMesa's internal converter framework to convert files into
geospatially-enabled formats and pass them along as a flow to be used by other processors in NiFi. The
following output formats are supported: Arrow, Avro, 16-byte binary encoded, CSV, GML 2 and 3, JSON,
Leaflet (HTML), Orc, Parquet, and TSV.

The ``ConvertToGeoFile`` processor also supports the converter properties detailed in :ref:`nifi_converter_processors`.

+-----------------------+-------------------------------------------------------------------------------------------+
| Property              | Description                                                                               |
+=======================+===========================================================================================+
| ``Output format``     | The output format to use                                                                  |
+-----------------------+-------------------------------------------------------------------------------------------+
| ``GZIP level``        | Level of gzip compression to apply to output, from 1-9                                    |
+-----------------------+-------------------------------------------------------------------------------------------+
| ``Include headers``   | Include header line in delimited export formats (CSV and TSV)                             |
+-----------------------+-------------------------------------------------------------------------------------------+

.. note::

  The ``ConvertToGeoAvro`` processor has been deprecated and replaced with the more flexible ``ConvertToGeoFile``
  processor.

GeoAvro Record Writer
~~~~~~~~~~~~~~~~~~~~~

GeoMesa also provides a record writer which will write out data in the GeoAvro format.
This component is configured as a service and then configured on a Processor which requires a record writer.
The configuration mirrors that for the :ref:`nifi_record_input_configuration`.
