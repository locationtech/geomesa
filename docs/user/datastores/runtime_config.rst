.. _geomesa_site_xml:

Runtime Configuration
=====================

GeoMesa uses system properties for various runtime configuration options. As a convenience, properties
can be specified in an XML file instead of the command line. When run, GeoMesa will load
the file ``geomesa-site.xml`` from the classpath (if available), and use any properties configured there.

To configure a GeoMesa binary distribution, place ``geomesa-site.xml`` in the ``conf`` folder.
For GeoServer, place the file under ``geoserver/WEB-INF/classes``. For other environments,
ensure the file is available at the root level of the classpath.

Each tools distribution contains a template file with the default settings at
``conf/geomesa-site.xml.template``. Do not modify this file directly as it is never read;
instead copy the desired configurations into ``geomesa-site.xml``.

By default, system properties set through command line parameters will take precedence over the
configuration file. If you wish a configuration item to always take precedence, even over command
line parameters, change the ``<final>`` tag to true.

Configuration properties with empty values will not be applied, you can change this by marking a
property as final.

Common Properties
-----------------

These properties apply to all GeoMesa implementations. Additional properties for different back-end
databases can be found in the chapters for each one.

geomesa.force.count
+++++++++++++++++++

This property controls how GeoMesa calculates the size of a result set (e.g. ``FeatureSource.getCount``).
By default, GeoMesa will estimate the size of a result set using statistics. This will provide a
rough estimate very quickly. Some applications rely on knowing the exact size of a result set up
front, so estimates will cause problems. To force GeoMesa to calculate the exact size of a result
set, you may set this property to "true". You may also override this behavior on a per-query basis
by using the query hint ``org.locationtech.geomesa.accumulo.index.QueryHints.EXACT_COUNT``.

geomesa.query.timeout.millis
++++++++++++++++++++++++++++

This property can be used to prevent long-running queries from overloading the system. When set,
queries will be closed after the timeout, even if not all results have been returned yet. The
timeout is specified in milliseconds.

geomesa.scan.ranges.target
++++++++++++++++++++++++++

This property provides a rough upper-limit for the number of row ranges that will be scanned for a single
query. In general, more ranges will result in fewer false-positive rows being scanned, which will speed up
most queries. However, too many ranges can take a long time to generate, and overwhelm clients, causing
slowdowns. The optimal value depends on the environment. Note that for temporal queries against the
Z3 or XZ3 index, the number of ranges will be multiplied by the number of time periods (e.g. weeks by
default) being queried.

geomesa.stats.generate
++++++++++++++++++++++

This property controls whether GeoMesa will generate statistics during ingestion. This property will be used
if a data store is not explicitly configured using the ``generateStats`` parameter.

geomesa.query.cost.type
+++++++++++++++++++++++

This property controls how GeoMesa performs query planning. By default, GeoMesa will perform cost-based
query planning using data statistics to determine the best index for a given query. As a fallback option,
this property may be set to "index" to use heuristic-based query planning. This may also be overridden on a
per-query basis using the query hint ``org.locationtech.geomesa.accumulo.index.QueryHints.COST_EVALUATION_KEY``
set to either ``org.locationtech.geomesa.accumulo.index.QueryPlanner.CostEvaluation.Stats``
or ``org.locationtech.geomesa.accumulo.index.QueryPlanner.CostEvaluation.Index``.

geomesa.strategy.decider
++++++++++++++++++++++++

This property allows for overriding strategy selection during query planning. It should specify the
full class name for a class implementing ``org.locationtech.geomesa.index.planning.StrategyDecider``.
The class must have a no-arg constructor.

By default GeoMesa will use cost-based query planning, which should work well for most situations.

geomesa.feature.id-generator
++++++++++++++++++++++++++++

This property controls the default implementation used for generating IDs for simple features,
if the ``USE_PROVIDED_FIDS`` or ``PROVIDED_FID`` hint is not set in the feature. It should be set to
the fully-qualified class name for a class implementing ``org.locationtech.geomesa.utils.uuid.FeatureIdGenerator``.

geomesa.audit.provider.impl
+++++++++++++++++++++++++++

GeoMesa provides a Java SPI for auditing queries. This property specifies the fully-qualified
class name of the audit provider implementation to use. For more information, see :ref:`audit_provider`.

geomesa.convert.scripts.path
++++++++++++++++++++++++++++

This property allows for adding files onto the classpath. It should be set to a colon-separated list of file
paths. This is useful for getting scripts onto the classpath for use by map-reduce ingest jobs.

geomesa.convert.config.urls
+++++++++++++++++++++++++++

This property allows for adding GeoMesa converter configurations to the environment. It can be set to a
comma-separated list of arbitrary URLs. For more information on converters, see :ref:`converters`.

geomesa.sft.config.urls
+++++++++++++++++++++++

This property allows for adding GeoMesa simple feature type configurations to the environment. It can be set to
a comma-separated list of arbitrary URLs.
