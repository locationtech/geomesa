Avro Processors
---------------

GeoAvro processors (``AvroToPutGeoMesa``) accept the following configuration parameters for specifying the
input source. Each datastore-specific processor also has additional parameters for connecting to the datastore,
detailed in the following sections.

+-------------------------+-------------------------------------------------------------------------------------------+
| Property                | Description                                                                               |
+=========================+===========================================================================================+
| ``Avro SFT match mode`` | Determines how Avro SimpleFeatureType mismatches are handled.                             |
+-------------------------+-------------------------------------------------------------------------------------------+

The SimpleFeatureTypes in GeoAvro may or may not match the SimpleFeatureType in the target datastore.
To address this, the AvroToPut processors have a property to set the SFT match mode. It can either be set to
an exact match ("by attribute number and order") or a more lenient one ("by attribute name"). The latter setting
will not write fields which are not in the target SFT.
