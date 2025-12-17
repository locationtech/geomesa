Micrometer Metrics
==================

GeoMesa provide built-in :ref:`geomesa_metrics` integrations. However, registries can also be managed programmatically, which
allows for greater flexibility and usage outside the normal GeoMesa workflows. Configuration is detailed in :ref:`geomesa_metrics`.

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

        // call close() on the result to deregister the server
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

        // call close() on the result to deregister the server
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

        // call close() on the result to deregister the server
        // this call uses environment variables to configure the registry (see below)
        Closeable registry = CloudwatchFactory.register();
        // or, for full control pass in a configuration
        Closeable registry2 = CloudwatchFactory.register(ConfigFactory.load("my-metrics-config"));

    .. code-tab:: scala

        import com.typesafe.config.ConfigFactory
        import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory

        // call close() on the result to deregister the server
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
