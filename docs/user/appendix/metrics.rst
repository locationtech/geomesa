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

For standard setups, Prometheus can be configured through environment variables, using the following call:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusSetup;

        PrometheusSetup.configure();

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusSetup

        PrometheusSetup.configure()

The following dependencies are required:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-metrics-micrometer_2.12</artifactId>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-prometheus</artifactId>
      <version>1.13.4</version>
    </dependency>
    <dependency>
      <groupId>io.prometheus</groupId>
      <artifactId>prometheus-metrics-exporter-httpserver</artifactId>
      <version>1.3.1</version>
    </dependency>

The following environment variables are supported:

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

For standard setups, Cloudwatch can be configured through environment variables, using the following call:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchSetup;

        CloudwatchSetup.configure();

    .. code-tab:: scala

        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchSetup

        CloudwatchSetup.configure()


The following dependencies are required:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-metrics-micrometer_2.12</artifactId>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-cloudwatch2</artifactId>
      <version>1.13.4</version>
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
