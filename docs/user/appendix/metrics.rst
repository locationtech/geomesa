.. _geomesa_metrics:

GeoMesa Metrics
===============

GeoMesa provides integration with the `DropWizard Metrics <http://metrics.dropwizard.io/>`__ library for real-time
reporting with the ``geomesa-metrics`` module.

Reporters are available for `SLF4J <https://www.slf4j.org/>`__, `CloudWatch <https://aws.amazon.com/cloudwatch/>`__,
`Graphite <https://graphiteapp.org/>`__, and `Ganglia <http://ganglia.sourceforge.net/>`__.

Configuration
-------------

Reporters are configured via TypeSafe Config. All reporters share a few common properties:

====================== ===============================================================================================
Configuration Property Description
====================== ===============================================================================================
``rate-units``         The Java TimeUnit_ used to report rates, e.g ``seconds``, ``minutes``, etc. For example, for a
                       requests meter, you can configure it to show requests per second or requests per hour
``duration-units``     The Java TimeUnit_ used to report durations, e.g. ``seconds``, ``milliseconds``, etc. For
                       example, for a request timer, you can configure it to show the time taken in seconds or
                       milliseconds
``units``              A fallback to use if ``rate-units`` and/or ``duration-units`` are not specified, which can
                       simplify the configuration
``interval``           How often the reporter should run, e.g. ``60 seconds`` or ``10 minutes``. For example, a
                       logging reporter will write a log message once per interval
====================== ===============================================================================================

.. _TimeUnit: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/TimeUnit.html

Logging Reporter
----------------

GeoMesa includes a logging reporter using SLF4J.

====================== ===============================================================================================
Configuration Property Description
====================== ===============================================================================================
``type``               Must be ``slf4j``
``logger``             The name of the logger to use, e.g. ``org.locationtech.geomesa.metrics``
``level``              The level to write out log messages at, e.g. ``info``, ``debug``, etc
====================== ===============================================================================================

Example configuration:

::

  {
    type     = "slf4j"
    units    = "milliseconds"
    interval = "60 seconds"
    logger   = "org.locationtech.geomesa.metrics"
    level    = "debug"
  }

CloudWatch Reporter
-------------------

The CloudWatch reporter can be included by adding a dependency on
``org.locationtech.geomesa:geomesa-metrics-cloudwatch_2.11``.  The CloudWatch reporter uses the default credentials
and region specified in your AWS profile config.

====================== ===============================================================================================
Configuration Property Description
====================== ===============================================================================================
``type``               Must be ``cloudwatch``
``namespace``          The CloudWatch namespace to use
``raw-counts``         Boolean - report the raw value of count metrics instead of reporting only the count difference
                       since the last report
``zero-values``        Boolean - POSTs to CloudWatch all values. Otherwise, the reporter does not POST values which
                       are zero in order to save costs
====================== ===============================================================================================

Example configuration:

::

  {
    type        = "cloudwatch"
    units       = "milliseconds"
    interval    = "60 seconds"
    namespace   = "mynamespace"
    raw-counts  = false
    zero-values = false
  }

Ganglia Reporter
----------------

The Ganglia reporter can be included by adding a dependency on
``org.locationtech.geomesa:geomesa-metrics-ganglia_2.11``. Using Ganglia requires additional GPL-licensed
dependencies ``info.ganglia.gmetric4j:gmetric4j:1.0.7`` and ``org.acplt:oncrpc:1.0.7``, which are excluded by default.

====================== ===============================================================================================
Configuration Property Description
====================== ===============================================================================================
``type``               Must be ``ganglia``
``group``              The host/group to send events to
``port``               Integer - The port to send events to
``addressing-mode``    One of ``multicast`` or ``unicast``
``ttl``                Integer - the time-to-live for Ganglia messages
``ganglia311``         Boolean - defines the Ganglia protocol version, either v3.1 or v3.0
====================== ===============================================================================================

Example configuration:

::

  {
    type            = "ganglia"
    group           = "example"
    port            = 8649
    addressing-mode = "multicast"
    ttl             = 32
    ganglia311      = true
    rate-units      = "seconds"
    duration-units  = "milliseconds"
    interval        = "10 seconds"
  }

Graphite Reporter
-----------------

The Graphite reporter can be included by adding a dependency on
``org.locationtech.geomesa:geomesa-metrics-graphite_2.11``.

====================== ===============================================================================================
Configuration Property Description
====================== ===============================================================================================
``type``               Must be ``graphite``
``url``                The connection string to the Graphite instance
``prefix``             Prefix prepended to all metric names
====================== ===============================================================================================

Example configuration:

::

  {
    type           = "graphite"
    url            = "localhost:9000"
    prefix         = "example"
    rate-units     = "seconds"
    duration-units = "milliseconds"
    interval       = "10 seconds"
  }

Extensions
----------

Additional reporters can be added at runtime by implementing
``org.locationtech.geomesa.metrics.core.ReporterFactory`` and registering the new class as a
`service provider <http://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__.
