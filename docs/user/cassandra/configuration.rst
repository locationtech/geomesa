Cassandra Configuration
=======================

This section details Cassandra-specific configuration properties. For general properties,
see :ref:`runtime_configuration`.

Connection Properties
---------------------

The Cassandra session is created with the Cassandra Java driver, which reads its settings (for example connection
and request timeouts) from the ``datastax-java-driver`` block of any ``application.conf`` file on the classpath.
For the GeoMesa command-line tools, this is ``conf/application.conf`` in the binary distribution. For example,
to increase the request timeout:

.. code-block:: none

    datastax-java-driver {
      basic.request.timeout = 10 seconds
    }

See the `driver documentation`__ for all available settings.

__ https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/configuration/index.html

geomesa.cassandra.schema.timeout
++++++++++++++++++++++++++++++++

Sets the timeout used for schema operations, i.e. creating, dropping or truncating tables. Cassandra applies schema
changes one at a time, and they have to be agreed on by all nodes in the cluster (dropping a table also snapshots it,
by default), so they can take much longer than regular requests. The timeout is defined as a duration, e.g.
``60 seconds`` or ``100 millis``. Default is ``2 minutes``.
