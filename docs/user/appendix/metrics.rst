GeoMesa Metrics
===============

.. warning::

  GeoMesa Metrics is deprecated and will be removed in a future version.

GeoMesa provides integration with the `DropWizard
Metrics <http://metrics.dropwizard.io/>`__ library for real-time
reporting with the ``geomesa-metrics`` module.

Instrumentation of GeoServer
----------------------------

GeoMesa metrics provides a servlet filter that will instrument GeoServer
requests.

Installation
------------

Copy the following jars into geoserver's ``WEB-INF/lib`` directory:

-  geomesa-metrics-.jar
-  metrics-core-3.1.2.jar
-  metrics-graphite-3.1.2.jar
-  metrics-ganglia-3.1.2.jar

Also copy the following jars if not already present:

-  scala-library-2.11.7.jar
-  scala-logging\_2.11-3.1.0.jar
-  scala-reflect-2.11.7.jar
-  config-1.2.1.jar
-  joda-time-2.3.jar


Ganglia Reporting
~~~~~~~~~~~~~~~~~

To enable ganglia reporters, also copy the following jars:

-  gmetric4j-1.0.7.jar
-  oncrpc-1.0.7.jar

Delimited File Reporting
~~~~~~~~~~~~~~~~~~~~~~~~

To enable TSV/CSV reporters, also copy the following jars:

-  commons-csv-1.0.jar

Configuration
-------------

To configure instrumentation, add the metrics filter to GeoServer's
``WEB-INF/web.xml``:

.. code-block:: xml

    <filter>
      <filter-name>metricsFilter</filter-name>
      <filter-class>org.locationtech.geomesa.metrics.servlet.AggregatedMetricsFilter</filter-class>
      <!-- name used for registering metrics - can also be specified in the config, but this takes priority -->
      <init-param>
        <param-name>name-prefix</param-name>
        <param-value>geoserver</param-value>
      </init-param>
      <!-- can embed configuration directly, otherwise will load default
           configurations and look under 'geomesa.metrics.servlet' -->
      <init-param>
        <param-name>config</param-name>
        <param-value>
        {
          // name used for registering metrics - init-param takes precedence
          name-prefix = "geoserver"
          // will only instrument matching urls
          // example string matched against: '/geoserver/&lt;workspace&gt;/ows'
          url-patterns = [ "(?i).*/(ows|wfs|wms)" ]
          // match request mappings based on just the workspace:layer name, or the whole url
          // if you are instrumenting non-wfs/wms requests, layer names will never match
          map-by-layer = true
          // mappings of urls to metric groups, based on regexes
          // any urls not mapped will end up under 'other'
          request-mappings = [
            {
              name = "qs"
              regex = ".*:AccumuloQuickStart"
            }
          ]
          // metrics reporters - see configuration details below
          reporters = {
            console = {
              type = "console"
              rate-units = "MINUTES"
              duration-units = "MILLISECONDS"
              interval = 10
            }
          }
        }
        </param-value>
      </init-param>
    </filter>

    <filter-mapping>
      <filter-name>metricsFilter</filter-name>
      <url-pattern>/*</url-pattern>
    </filter-mapping>

Session Tracking
----------------

The metrics servlet doesn't track active user sessions by default.
GeoServer mostly doesn't create sessions for OWS requests, and blatantly
warns you against doing so. However, if you are willing to incur the
cost of session management, you may enable session tracking in the
metrics servlet.

Update the configuration for the metrics servlet (either in
``application.conf`` or ``web.xml``) with the following attribute:

.. code-block:: javascript

    // how often to update metrics for expired sessions, in seconds
    // if set to &lt; 1, sessions will not be tracked
    // use in conjunction with the session listener defined below
    session-removal-interval = 60

Add the following listener to GeoServer's ``WEB-INF/web.xml``:

.. warning::

    Failure to add this listener when session tracking is
    enabled will cause incorrect metrics reports and eventually lead to
    out-of-memory errors

.. code-block:: xml

    <!-- listener for sessions events
         if you enable session tracking and this is not defined, sessions will never
         be expired from the metrics cache and you will eventually run out of memory -->
    <listener>
      <listener-class>org.locationtech.geomesa.metrics.servlet.SessionMetricsListener</listener-class>
    </listener>

In order to suppress GeoServer's warnings about session creation,
comment out the following filter in GeoServer's ``WEB-INF/web.xml``:

.. code-block:: xml

    <!--
    <filter>
      <filter-name>SessionDebugger</filter-name>
      <filter-class>org.geoserver.filters.SessionDebugFilter</filter-class>
    </filter>
    -->
    ...
    <!--
    <filter-mapping>
      <filter-name>SessionDebugger</filter-name>
      <url-pattern>/*</url-pattern>
    </filter-mapping>
    -->

Configuration of Reporters
--------------------------

Use ``org.locationtech.geomesa.metrics.config.MetricsConfig.reporters``
to configure reporters via TypeSafe Config. Reporters should be defined
as objects under the path ``geomesa.metrics.reporters``:

::

    geomesa = {
      metrics = {
        reporters = {
          console = {
            type     = "console"
            units    = "MILLISECONDS"
            interval = 60
          }
          slf4j = {
            type     = "slf4j"
            units    = "MILLISECONDS"
            interval = 60
            logger   = "org.locationtech.geomesa"
            level    = "debug"
          }
          delimited-text = {
            type      = "delimited-text"
            units     = "MILLISECONDS"
            interval  = 60
            tabs      = true
            aggregate = true
            output    = ${java.io.tmpdir}/"geoserver-metrics"
          }
          graphite = {
            type     = "graphite"
            units    = "MILLISECONDS"
            interval = 60
            url      = "graphite.example.com:80"
            prefix   = "org.locationtech.geomesa"
          }
          ganglia = {
            type            = "ganglia"
            units           = "MILLISECONDS"
            interval        = 60
            group           = "ganglia.example.com"
            port            = 8649
            addressing-mode = "MULTICAST"
            ttl             = 1
            ganglia311      = true
          }
          accumulo = {
            type       = "accumulo"
            units      = "MILLISECONDS"
            interval   = -1
            instanceId = "mycloud"
            zookeepers = "zoo1,zoo2,zoo3"
            user       = "myuser"
            password   = "mypassword"
            tableName  = "geomesa_metrics"
          }      
        }
      }
    }

Standard Configuration
----------------------

The following fields are common among all reporters:

+----------------------+-----------+---------------------------------------------------------------------------------------------------------------+
| Field                | Type      | Description                                                                                                   |
+======================+===========+===============================================================================================================+
| ``rate-units``       | String    | The type of units used to report the rate of a metric. Corresponds to ``java.util.concurrent.TimeUnit``       |
+----------------------+-----------+---------------------------------------------------------------------------------------------------------------+
| ``duration-units``   | String    | The type of units used to report the duration of a metric. Corresponds to ``java.util.concurrent.TimeUnit``   |
+----------------------+-----------+---------------------------------------------------------------------------------------------------------------+
| ``units``            | String    | If rate or duration units are not specified, this will be used instead.                                       |
+----------------------+-----------+---------------------------------------------------------------------------------------------------------------+
| ``interval``         | Integer   | How often the reporter will run, in seconds. If less than 1, reporter will not run automatically.             |
+----------------------+-----------+---------------------------------------------------------------------------------------------------------------+
| ``type``             | String    | The type of reporter. Types are documented below.                                                             |
+----------------------+-----------+---------------------------------------------------------------------------------------------------------------+

Console Reporter
~~~~~~~~~~~~~~~~

Writes metrics to the console.

+------------+----------+-----------------------+
| Field      | Type     | Description           |
+============+==========+=======================+
| ``type``   | String   | Must be ``console``   |
+------------+----------+-----------------------+

Slf4j Reporter
~~~~~~~~~~~~~~

Writes metrics using an slf4j logger.

+--------------+----------+---------------------------------------------------------------------------------------------------------------------------------+
| Field        | Type     | Description                                                                                                                     |
+==============+==========+=================================================================================================================================+
| ``type``     | String   | Must be ``slf4j``                                                                                                               |
+--------------+----------+---------------------------------------------------------------------------------------------------------------------------------+
| ``logger``   | String   | The name of the logger that will be used for logging.                                                                           |
+--------------+----------+---------------------------------------------------------------------------------------------------------------------------------+
| ``level``    | String   | (optional) Level to use for logger messages. One of ``trace``, ``debug``, ``info``, ``warn``, ``error``. Default is ``debug``   |
+--------------+----------+---------------------------------------------------------------------------------------------------------------------------------+

Delimited Text Reporter
~~~~~~~~~~~~~~~~~~~~~~~

Writes metrics to tab or comma-delimited files.

+-----------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Field           | Type      | Description                                                                                                                                                   |
+=================+===========+===============================================================================================================================================================+
| ``type``        | String    | Must be ``delimited-text``                                                                                                                                    |
+-----------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``output``      | String    | The path to output metrics to. Will be passed into ``new java.io.File(output)``                                                                               |
+-----------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``aggregate``   | Boolean   | (optional) Aggregate output files by type. If true, there will be one file per metric type; if false there will be one file per metric. Default is ``true``   |
+-----------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``tabs``        | Boolean   | (optional) If true, delimit entries with tabs, else delimit entries with commas. Default is ``true``                                                          |
+-----------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+

Graphite Reporter
~~~~~~~~~~~~~~~~~

Writes metrics to Graphite.

+--------------+----------+--------------------------------------------------------------------+
| Field        | Type     | Description                                                        |
+==============+==========+====================================================================+
| ``type``     | String   | Must be ``graphite``                                               |
+--------------+----------+--------------------------------------------------------------------+
| ``url``      | String   | The URL to the graphite server, in the form of ``<host>:<port>``   |
+--------------+----------+--------------------------------------------------------------------+
| ``prefix``   | String   | (optional) The graphite prefix to use                              |
+--------------+----------+--------------------------------------------------------------------+

Ganglia Reporter
~~~~~~~~~~~~~~~~

Writes metrics to Ganglia.

+-----------------------+-----------+----------------------------------------------------------------------------------+
| Field                 | Type      | Description                                                                      |
+=======================+===========+==================================================================================+
| ``type``              | String    | Must be ``ganglia``                                                              |
+-----------------------+-----------+----------------------------------------------------------------------------------+
| ``group``             | String    | The group (url) used for connecting to the ganglia server                        |
+-----------------------+-----------+----------------------------------------------------------------------------------+
| ``port``              | Int       | The port used for connecting to the ganglia server                               |
+-----------------------+-----------+----------------------------------------------------------------------------------+
| ``ttl``               | Int       | Time-to-live for broadcast packets, in the range of 0-255                        |
+-----------------------+-----------+----------------------------------------------------------------------------------+
| ``addressing-mode``   | String    | (optional) Addressing mode to use. Must be one of ``unicast`` or ``multicast``   |
+-----------------------+-----------+----------------------------------------------------------------------------------+
| ``ganglia311``        | Boolean   | (optional) To use protocol version 3.1 (true) or 3.0 (false). Default is 3.1     |
+-----------------------+-----------+----------------------------------------------------------------------------------+

Accumulo Reporter
~~~~~~~~~~~~~~~~~

Writes metrics to Accumulo.

+--------------------+----------+------------------------------------------------------------+
| Field              | Type     | Description                                                |
+====================+==========+============================================================+
| ``type``           | String   | Must be ``accumulo``                                       |
+--------------------+----------+------------------------------------------------------------+
| ``instanceId``     | String   | The instance ID for the accumulo cluster                   |
+--------------------+----------+------------------------------------------------------------+
| ``zookeepers``     | String   | The zookeeper connection string for the accumulo cluster   |
+--------------------+----------+------------------------------------------------------------+
| ``user``           | String   | The accumulo user to connect with                          |
+--------------------+----------+------------------------------------------------------------+
| ``password``       | String   | The password for the accumulo user                         |
+--------------------+----------+------------------------------------------------------------+
| ``tableName``      | String   | The table metrics will be written to                       |
+--------------------+----------+------------------------------------------------------------+
| ``visibilities``   | String   | (optional) Visibilities applied to written data            |
+--------------------+----------+------------------------------------------------------------+
