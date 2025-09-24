.. _geomesa_metrics:

Micrometer Metrics
==================

GeoMesa has initial support for `Micrometer <https://docs.micrometer.io/micrometer/reference/>`__ metrics. Currently,
metrics can integrate with either Prometheus or Cloudwatch. Creating registries is idempotent, as long as the
configuration does not change.

Most GeoMesa data stores support the parameters ``geomesa.metrics.registry`` and ``geomesa.metrics.registry.config``, which are
used to start up a registry for publishing metrics. Configuration is described below.

When enabled, various GeoMesa components will publish metrics, including the :ref:`kafka_index` and :ref:`converters`.

Prometheus Registry
-------------------

The Prometheus registry can be configured through the following environment variables:

+--------------------------------------+---------+----------------------------------------------+
| Environment Variable                 | Default | Description                                  |
+======================================+=========+==============================================+
| ``RENAME_PROMETHEUS_METRICS``        | true    | `Rename`__ metrics                           |
+--------------------------------------+---------+----------------------------------------------+
| ``METRICS_APPLICATION_NAME``         | geomesa | Add a tag for ``application`` to all metrics |
+--------------------------------------+---------+----------------------------------------------+
| ``METRICS_PORT``                     | 9090    | Set the port used to host metrics            |
+--------------------------------------+---------+----------------------------------------------+

__ https://docs.micrometer.io/micrometer/reference/implementations/prometheus.html#_the_prometheus_rename_filter

Instead of environment variables, the registry can be fully configured using using the data store parameter
``geomesa.metrics.registry.config``, which takes `Lightbend Config <https://github.com/lightbend/config/tree/main>`__:

::

    # port used to serve metrics - not used if push-gateway is defined
    port = 9090
    # tags applied to all metrics, may be any key-value string pairs
    common-tags = { "application" = "my-app" }
    # use prometheus "standard" names - see https://docs.micrometer.io/micrometer/reference/implementations/prometheus.html#_the_prometheus_rename_filter
    rename = false
    # additional config - can also be done via sys props, see https://prometheus.github.io/client_java/config/config/
    properties = {}
    # optional - enable pushgateway for short-lived jobs, instead of the standard metrics server for scraping
    push-gateway = {
      host = "localhost:9091"
      job = "my-job"
      scheme = "http"
      format = "PROMETHEUS_PROTOBUF" # or PROMETHEUS_TEXT
    }

If using Pushgateway, the following dependency is required:

.. code-block:: xml

    <dependency>
      <groupId>io.prometheus</groupId>
      <artifactId>prometheus-metrics-exporter-pushgateway</artifactId>
      <version>{{prometheus_version}}</version>
    </dependency>

Cloudwatch Registry
-------------------

The Cloudwatch registry can be configured through the following environment variables:

+--------------------------------------+---------+----------------------------------------------+
| Environment Variable                 | Default | Description                                  |
+======================================+=========+==============================================+
| ``METRICS_NAMESPACE``                | geomesa | Cloudwatch namespace                         |
+--------------------------------------+---------+----------------------------------------------+

Instead of environment variables, the registry can be fully configured using using the data store parameter
``geomesa.metrics.registry.config``, which takes `Lightbend Config <https://github.com/lightbend/config/tree/main>`__:

::

    # cloudwatch namespace
    namespace = "geomesa"
    # properties for the cloudwatch client
    properties = {}

For Cloudwatch, the following dependency is required:

.. code-block:: xml

    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-cloudwatch2</artifactId>
      <version>{{micrometer_version}}</version>
    </dependency>

Instrumentations
----------------

When adding a registry, additional JVM metrics can be exposed with the following environment variables:

+--------------------------------------+---------+----------------------------------------------+
| Environment Variable                 | Default | Description                                  |
+======================================+=========+==============================================+
| ``METRICS_INSTRUMENTATIONS_ENABLED`` | true    | Enable all JVM instrumentations              |
+--------------------------------------+---------+----------------------------------------------+

Individual instrumentations can be enabled or disabled through the following environment variables:

+--------------------------------------+---------+----------------------------------------------+
| Environment Variable                 | Default | Description                                  |
+======================================+=========+==============================================+
| ``METRICS_INSTRUMENT_CLASSLOADER``   |         | Enable metrics on JVM class loading          |
+--------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_MEMORY``        |         | Enable metrics on JVM memory usage           |
+--------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_GC``            |         | Enable metrics on JVM garbage collection     |
+--------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_PROCESSOR``     |         | Enable metrics on processor usage            |
+--------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_THREADS``       |         | Enable metrics on JVM thread usage           |
+--------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_COMMONS_POOL``  |         | Enable metrics on Apache commons-pool pools  |
+--------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_POSTGRES``      |         | Enable metrics on PostgreSQL                 |
+--------------------------------------+---------+----------------------------------------------+

Or, when using ``geomesa.metrics.registry.config``, use the following keys:

::

    # tags are key-value string pairs added to metrics from the given instrumentation
    classloader  = { enabled = true, tags = {} }
    memory       = { enabled = true, tags = {} }
    gc           = { enabled = true, tags = {} }
    processor    = { enabled = true, tags = {} }
    threads      = { enabled = true, tags = {} }
    commons-pool = { enabled = true, tags = {} }
    postgres     = { enabled = true, tags = {} }

.. note::

    PostgreSQL metrics are only available in the :ref:`postgis_index_page`.

Standalone Usage
----------------

Registries can also be managed programmatically. This allows for greater flexibility and usage outside the normal GeoMesa
workflows. Configuration can be managed as detailed above.

Prometheus
^^^^^^^^^^

To create a Prometheus registry, use the following call:

.. tabs::

    .. code-tab:: java

        import com.typesafe.config.ConfigFactory;
        import io.micrometer.core.instrument.MeterRegistry;
        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory;

        // configuration is read from environment variables
        MeterRegistry registry = PrometheusFactory.apply();
        // or, alternatively pass in a configuration
        MeterRegistry registry2 = PrometheusFactory.apply(ConfigFactory.load("my-metrics-config"));

    .. code-tab:: scala

        import com.typesafe.config.ConfigFactory
        import io.micrometer.core.instrument.MeterRegistry
        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory

        // configuration is read from environment variables
        val registry: MeterRegistry = PrometheusFactory()
        // or, alternatively pass in a configuration
        val registry2: MeterRegistry = PrometheusFactory(ConfigFactory.load("my-metrics-config"))

To idempotently create a Prometheus registry and attach it to the global registry, use the following call:

.. tabs::

    .. code-tab:: java

        import com.typesafe.config.ConfigFactory;
        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory;

        // call close() on the result to shut down the server
        // this call uses environment variables to configure the registry
        Closeable registry = PrometheusFactory.register();
        // alternatively, use the method that accepts configuration directly
        int port = 9090;
        String applicationTag = "myApp";
        boolean renameMetrics = true;
        Closeable registry2 = PrometheusFactory.register(port, applicationTag, renameMetrics);
        // or, for full control pass in a configuration
        Closeable registry3 = PrometheusFactory.register(ConfigFactory.load("my-metrics-config"));

    .. code-tab:: scala

        import com.typesafe.config.ConfigFactory
        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory

        // call close() on the result to shut down the server
        // this call uses environment variables to configure the registry
        val registry: Closeable = PrometheusFactory.register()
        // alternatively, use the method that accepts configuration directly
        val port = 9090
        val applicationTag = "myApp"
        val renameMetrics = true
        val registry2: Closeable = PrometheusFactory.register(port, applicationTag, renameMetrics)
        // or, for full control pass in a configuration
        val registry3: Closeable = PrometheusFactory.register(ConfigFactory.load("my-metrics-config"))

For Pushgateway, use:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory;
        import io.prometheus.metrics.exporter.pushgateway.Format;
        import io.prometheus.metrics.exporter.pushgateway.Scheme;

        String host = "pushgateway:9091";
        String job = "my-job";
        Scheme scheme = Scheme.HTTP;
        Format format = Format.PROMETHEUS_PROTOBUF;
        String applicationTag = "myApp";
        boolean renameMetrics = true;
        // call close() on the result to send metrics to the gateway
        Closeable registry = PrometheusFactory.registerPushGateway(host, job, scheme, format, applicationTag, renameMetrics);

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory
        import io.prometheus.metrics.exporter.pushgateway.{Format, Scheme}

        val host = "pushgateway:9091"
        val job = "my-job"
        val scheme = Scheme.HTTP
        val format = Format.PROMETHEUS_PROTOBUF
        val applicationTag = "myApp"
        val renameMetrics = true
        // call close() on the result to send metrics to the gateway
        val registry: Closeable = PrometheusFactory.registerPushGateway(host, job, scheme, format, applicationTag, renameMetrics)

Cloudwatch
^^^^^^^^^^

To create a Cloudwatch registry, use the following call:

.. tabs::

    .. code-tab:: java

        import com.typesafe.config.ConfigFactory;
        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory;

        // configuration is read from environment variables
        MeterRegistry registry = CloudwatchFactory.apply();
        // or, alternatively pass in a configuration
        MeterRegistry registry2 = CloudwatchFactory.apply(ConfigFactory.load("my-metrics-config"));

    .. code-tab:: scala

        import com.typesafe.config.ConfigFactory
        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory

        // configuration is read from environment variables
        val registry: MeterRegistry = CloudwatchFactory()
        // or, alternatively pass in a configuration
        val registry2: MeterRegistry = CloudwatchFactory(ConfigFactory.load("my-metrics-config"))

To idempotently start a Cloudwatch registry and attach it to the global registry, use the following call:

.. tabs::

    .. code-tab:: java

        import com.typesafe.config.ConfigFactory;
        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory;

        // call close() on the result to shut down the server
        // this call uses environment variables to configure the registry (see below)
        Closeable registry = CloudwatchFactory.register();
        // or, for full control pass in a configuration
        Closeable registry2 = CloudwatchFactory.register(ConfigFactory.load("my-metrics-config"));

    .. code-tab:: scala

        import com.typesafe.config.ConfigFactory
        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory

        // call close() on the result to shut down the server
        // this call uses environment variables to configure the registry (see below)
        val registry: Closeable = CloudwatchFactory.register()
        // or, for full control pass in a configuration
        val registry2: Closeable = CloudwatchFactory.register(ConfigFactory.load("my-metrics-config"))

Apache Commons DBCP2
--------------------

GeoMesa provides a metrics-enabled DataSource that can be used in place of an Apache DBCP2 ``BasicDataSource`` for connection
pooling. First, ensure that the ``commons-pool`` instrumentation is enabled (above), then use the data source as follows:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.dbcp2.MetricsDataSource;

        MetricsDataSource dataSource = new MetricsDataSource();

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.dbcp2.MetricsDataSource

        val dataSource = new MetricsDataSource()

Custom Registries
-----------------

If the provided registries are not sufficient, metrics can be exposed by programmatically adding any Micrometer registry to the
`global registry <https://docs.micrometer.io/micrometer/reference/concepts/registry.html#_global_registry>`__.
