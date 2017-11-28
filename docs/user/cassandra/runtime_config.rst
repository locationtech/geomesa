Cassandra Configuration
=======================

This section details Cassandra-specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

Connection Properties
---------------------

The following properties control the configuration of the Cassandra session.

geomesa.cassandra.connection.timeout
++++++++++++++++++++++++++++++++++++

Sets the connection timeout. The timeout is defined as a duration, e.g. ``60 seconds`` or ``100 millis``.
See the `Cassandra API`__ for details.

__ http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int-

geomesa.cassandra.read.timeout
++++++++++++++++++++++++++++++

Sets the read timeout. The timeout is defined as a duration, e.g. ``60 seconds`` or ``100 millis``.
See the `Cassandra API`__ for details.

__ http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int-
