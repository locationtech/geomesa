.. _geomesa_metrics:

GeoMesa Metrics
===============

GeoMesa provides integration with the `DropWizard Metrics <https://metrics.dropwizard.io/>`__ library for real-time
reporting with the ``geomesa-metrics`` module.

Reporters are available for `CloudWatch <https://aws.amazon.com/cloudwatch/>`__,
`Prometheus <https://prometheus.io/>`__, `Graphite <https://graphiteapp.org/>`__,
`Ganglia <https://ganglia.sourceforge.net/>`__, and `SLF4J <https://www.slf4j.org/>`__.

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
``org.locationtech.geomesa:geomesa-metrics-cloudwatch``.  The CloudWatch reporter uses the default credentials
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

Prometheus Reporter
-------------------

The Prometheus reporter can be included by adding a dependency on
``org.locationtech.geomesa:geomesa-metrics-prometheus``.  The Prometheus reporter supports normal Prometheus scraping
as well as the Prometheus Pushgateway. Note that the unit and interval configurations described above do not apply
to Prometheus reporters.

Prometheus Scraping
^^^^^^^^^^^^^^^^^^^

The standard Prometheus reporter will expose an HTTP endpoint to be scraped by Prometheus.

====================== ===============================================================================================
Configuration Property Description
====================== ===============================================================================================
``type``               Must be ``prometheus``
``port``               The port used to expose metrics
``suffix``             A suffix to append to all metric names
====================== ===============================================================================================

Example configuration:

::

  {
    type = "prometheus"
    port = "9090"
  }

Prometheus Pushgateway
^^^^^^^^^^^^^^^^^^^^^^

For short-lived jobs, metrics can be sent to a Prometheus Pushgateway instead of being exposed for scraping.

====================== ===============================================================================================
Configuration Property Description
====================== ===============================================================================================
``type``               Must be ``prometheus-pushgateway``
``gateway``            The Pushgateway host
``job-name``           The name of the batch job being run
``suffix``             A suffix to append to all metric names
====================== ===============================================================================================

Example configuration:

::

  {
    type     = "prometheus-pushgateway"
    gateway  = "http://pushgateway:8080/"
    job-name = "my-job"
  }

Ganglia Reporter
----------------

The Ganglia reporter can be included by adding a dependency on
``org.locationtech.geomesa:geomesa-metrics-ganglia``. Using Ganglia requires additional GPL-licensed
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
``org.locationtech.geomesa:geomesa-metrics-graphite``.

====================== ===============================================================================================
Configuration Property Description
====================== ===============================================================================================
``type``               Must be ``graphite``
``url``                The connection string to the Graphite instance
``prefix``             Prefix prepended to all metric names
``ssl``                Boolean to enable or disable SSL connections
====================== ===============================================================================================

Example configuration:

::

  {
    type           = "graphite"
    url            = "localhost:9000"
    ssl            = false
    prefix         = "example"
    rate-units     = "seconds"
    duration-units = "milliseconds"
    interval       = "10 seconds"
  }

If SSL is enabled, standard Java system properties can be used to control key stores and trust stores, i.e.
``javax.net.ssl.keyStore``, etc.

Extensions
----------

Additional reporters can be added at runtime by implementing
``org.locationtech.geomesa.metrics.core.ReporterFactory`` and registering the new class as a
`service provider <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__.
