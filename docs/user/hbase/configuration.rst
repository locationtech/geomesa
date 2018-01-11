.. _hbase_config_props:

HBase Configuration
===================

This section details HBase specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

geomesa.hbase.config.paths
++++++++++++++++++++++++++

Additional configuration file paths, comma-delimited. The files will be added to the HBase configuration prior
to creating a ``Connection``. This property will be overridden by the data store configuration parameter,
if both are specified.

geomesa.hbase.remote.filtering
++++++++++++++++++++++++++++++

Disable remote filtering. Remote filtering and coprocessors speed up queries, however they require the installation
of custom JARs in HBase. Since this is not always possible, they can be disabled by setting this to ``false``.
This property will be overridden by the data store configuration parameter, if both are specified.
