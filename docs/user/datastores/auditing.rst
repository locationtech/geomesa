.. _audit_provider:

Query Auditing
--------------

GeoMesa provides a Java SPI to audit queries. Auditing can be enabled when creating a ``DataStore`` by setting
the parameter ``geomesa.query.audit`` to ``true`` in the connection map. Audits are written to log files under
the logger name ``org.locationtech.geomesa.index.audit.AuditWriter``. In Accumulo, audits are also written
to the ``<catalog>_queries`` table.

As GeoMesa can run in many environments, determining **who** executed a query is delegated to a service class.
Services need to implement ``org.locationtech.geomesa.utils.audit.AuditProvider``. Third-party implementations
can be enabled by placing them on the classpath and including a special service descriptor file. See the
`Oracle Javadoc <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__
for details on implementing a service provider.

The GeoMesa GeoServer plugins come bundled with an ``AuditProvider`` that pulls user credentials from
GeoServer's Spring security framework - ``org.locationtech.geomesa.security.SpringAuditProvider``.
