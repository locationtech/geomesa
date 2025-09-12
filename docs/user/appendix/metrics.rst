.. _geomesa_metrics:

Micrometer Metrics
==================

GeoMesa has initial support for `Micrometer <https://docs.micrometer.io/micrometer/reference/>`__ metrics. Currently,
metrics can integrate with either Prometheus or Cloudwatch. Creating registries is idempotent, as long as the
configuration does not change.

When enabled, various GeoMesa components will publish metrics, including the :ref:`kafka_index` and :ref:`converters`.

If the provided registries are not sufficient, any Micrometer registry can be
`added to the global registry <https://docs.micrometer.io/micrometer/reference/concepts/registry.html#_global_registry>`__
programmatically.

Standard Usage
--------------

Prometheus
^^^^^^^^^^

A variety of JVM and application metrics can be exposed through Prometheus, using the following call:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusSetup;

        PrometheusSetup.configure();

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusSetup

        PrometheusSetup.configure()

To start a Prometheus registry and attach it to the global registry, without enabling JVM metrics, use the following call:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusSetup;

        // call close() on the result to shut down the server
        // this call uses environment variables to configure the registry (see below)
        Closeable registry = PrometheusSetup.register();
        // alternatively, use the method that accepts configuration directly
        int port = 9090;
        String applicationTag = "myApp";
        boolean renameMetrics = true;
        Closeable registry2 = PrometheusSetup.register(port, applicationTag, renameMetrics);

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusSetup

        // call close() on the result to shut down the server
        // this call uses environment variables to configure the registry (see below)
        val registry: Closeable = PrometheusSetup.register()
        // alternatively, use the method that accepts configuration directly
        val port = 9090
        val applicationTag = "myApp"
        val renameMetrics = true
        val registry2: Closeable = PrometheusSetup.register(port, applicationTag, renameMetrics)

For Pushgateway, use:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusSetup;
        import io.prometheus.metrics.exporter.pushgateway.Format;
        import io.prometheus.metrics.exporter.pushgateway.Scheme;

        String host = "pushgateway:9091";
        String job = "my-job";
        Scheme scheme = Scheme.HTTP;
        Format format = Format.PROMETHEUS_PROTOBUF;
        String applicationTag = "myApp";
        boolean renameMetrics = true;
        // call close() on the result to send metrics to the gateway
        Closeable registry = PrometheusSetup.registerPushGateway(host, job, scheme, format, applicationTag, renameMetrics);

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusSetup
        import io.prometheus.metrics.exporter.pushgateway.{Format, Scheme}

        val host = "pushgateway:9091"
        val job = "my-job"
        val scheme = Scheme.HTTP
        val format = Format.PROMETHEUS_PROTOBUF
        val applicationTag = "myApp"
        val renameMetrics = true
        // call close() on the result to send metrics to the gateway
        val registry: Closeable = PrometheusSetup.registerPushGateway(host, job, scheme, format, applicationTag, renameMetrics)

The following dependencies are required:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-metrics-micrometer_2.12</artifactId>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-prometheus</artifactId>
      <version>{{micrometer_version}}</version>
    </dependency>
    <dependency>
      <groupId>io.prometheus</groupId>
      <artifactId>prometheus-metrics-exporter-httpserver</artifactId>
      <version>{{prometheus_version}}</version>
    </dependency>

To customize behavior, the following environment variables are supported:

+-------------------------------------+---------+----------------------------------------------+
| Environment Variable                | Default | Description                                  |
+=====================================+=========+==============================================+
| ``METRICS_ENABLED``                 | true    | Enable metrics                               |
+-------------------------------------+---------+----------------------------------------------+
| ``RENAME_PROMETHEUS_METRICS``       | true    | `Rename`__ metrics                           |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_APPLICATION_NAME``        |         | Add a tag for ``application`` to all metrics |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_PORT``                    | 9090    | Set the port used to host metrics            |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_CLASSLOADER``  | true    | Enable metrics on JVM class loading          |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_MEMORY``       | true    | Enable metrics on JVM memory usage           |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_GC``           | true    | Enable metrics on JVM garbage collection     |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_PROCESSOR``    | true    | Enable metrics on processor usage            |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_THREADS``      | true    | Enable metrics on JVM thread usage           |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_COMMONS_POOL`` | true    | Enable metrics on Apache commons-pool pools  |
+-------------------------------------+---------+----------------------------------------------+

__ https://docs.micrometer.io/micrometer/reference/implementations/prometheus.html#_the_prometheus_rename_filter

For advanced configuration, see below.

Cloudwatch
^^^^^^^^^^

A variety of JVM and application metrics can be exposed through Cloudwatch, using the following call:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchSetup;

        CloudwatchSetup.configure();

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchSetup

        CloudwatchSetup.configure()

To start a Cloudwatch registry and attach it to the global registry, without enabling JVM metrics, use the following call:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchSetup;

        // call close() on the result to shut down the server
        // this call uses environment variables to configure the registry (see below)
        Closeable registry = CloudwatchSetup.register();

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchSetup

        // call close() on the result to shut down the server
        // this call uses environment variables to configure the registry (see below)
        val registry: Closeable = CloudwatchSetup.register()

The following dependencies are required:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-metrics-micrometer_2.12</artifactId>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-cloudwatch2</artifactId>
      <version>{{micrometer_version}}</version>
    </dependency>

The following environment variables are supported:

+-------------------------------------+---------+----------------------------------------------+
| Environment Variable                | Default | Description                                  |
+=====================================+=========+==============================================+
| ``METRICS_ENABLED``                 | true    | Enable metrics                               |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_NAMESPACE``               | geomesa | Cloudwatch namespace                         |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_CLASSLOADER``  | true    | Enable metrics on JVM class loading          |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_MEMORY``       | true    | Enable metrics on JVM memory usage           |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_GC``           | true    | Enable metrics on JVM garbage collection     |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_PROCESSOR``    | true    | Enable metrics on processor usage            |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_THREADS``      | true    | Enable metrics on JVM thread usage           |
+-------------------------------------+---------+----------------------------------------------+
| ``METRICS_INSTRUMENT_COMMONS_POOL`` | true    | Enable metrics on Apache commons-pool pools  |
+-------------------------------------+---------+----------------------------------------------+

For advanced configuration, see below.

Advanced Setup
--------------

For advanced configuration, metric implementations can be configured at runtime through
`Lightbend Config <https://github.com/lightbend/config/tree/main>`__, and enabled as follows:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.MicrometerSetup;

        MicrometerSetup.configure();

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.MicrometerSetup

        MicrometerSetup.configure()

Configuration should be under the key ``geomesa.metrics``, and takes the following config:

::

    geomesa.metrics = {
      registries = {
        # see below for registry configs
      }
      instrumentations = {
        # jvm classloader metrics
        classloader = {
            enabled = false
            tags = {}
        }
        # jvm memory usage metrics
        memory = {
          enabled = false
          tags = {}
        }
         # jvm garbage collection metrics
        gc = {
          enabled = false
          tags = {}
        }
         # jvm processor usage metrics
        processor = {
          enabled = false
          tags = {}
        }
        # jvm thread usage metrics
        threads = {
          enabled = false
          tags = {}
        }
        # apache commons-pool/dbcp metrics
        commons-pool = {
          enabled = false
          tags = {}
        }
      }
    }

Prometheus Registry
^^^^^^^^^^^^^^^^^^^

::

    # note: the top-level key here is only for uniqueness - it can be any string
    "prometheus" = {
      type = "prometheus"
      enabled = true
      # use prometheus "standard" names - see https://docs.micrometer.io/micrometer/reference/implementations/prometheus.html#_the_prometheus_rename_filter
      rename = false
      common-tags = { "application" = "my-app" }
      # port used to serve metrics - not used if push-gateway is defined
      port = 9090
      # additional config can also be done via sys props - see https://prometheus.github.io/client_java/config/config/
      properties = {}
      # optional - enable pushgateway for short-lived jobs, instead of the standard metrics server for scraping
      push-gateway = {
        host = "localhost:9091"
        job = "my-job"
        scheme = "http"
        format = "PROMETHEUS_PROTOBUF" # or PROMETHEUS_TEXT
      }
    }

Cloudwatch Registry
^^^^^^^^^^^^^^^^^^^

::

    # note: the top-level key here is only for uniqueness - it can be any string
    "cloudwatch" = {
      type = "cloudwatch"
      enabled = true
      namespace = "geomesa"
      # properties for the cloudwatch client
      properties = {}
    }

Apache Commons DBCP2
--------------------

GeoMesa provides a metrics-enabled DataSource that can be used in place of an Apache DBCP2 ``BasicDataSource`` for connection
pooling. First, ensure that ``commons-pool`` metrics are enabled (above), then use the data source as follows:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.dbcp2.MetricsDataSource;

        MetricsDataSource dataSource = new MetricsDataSource();

        dataSource.setUrl("jdbc:postgresql://postgres/postgres");
        dataSource.setUsername("postgres");
        dataSource.setPassword("postgres");

        // set pooling parameters as desired
        dataSource.setMaxTotal(10);

        // allows micrometer to instrument this data source
        dataSource.registerJmx();

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.dbcp2.MetricsDataSource

        val dataSource = new MetricsDataSource()

        dataSource.setUrl("jdbc:postgresql://postgres/postgres")
        dataSource.setUsername("postgres")
        dataSource.setPassword("postgres")

        // set pooling parameters as desired
        dataSource.setMaxTotal(10)

        // allows micrometer to instrument this data source
        dataSource.registerJmx()
