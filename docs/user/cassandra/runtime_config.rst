Cassandra Configuration
=======================

This section details Cassandra-specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

Connection Properties
---------------------

The following properties control the configuration of the Cassandra session.

geomesa.cassandra.connection.timeout.millis
+++++++++++++++++++++++++++++++++++++++++++

Sets the connection timeout. See the
`Cassandra API <http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int->`__

geomesa.cassandra.read.timeout.millis
+++++++++++++++++++++++++++++++++++++

Sets the read timeout. See the
`Cassandra API <http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int->`__
