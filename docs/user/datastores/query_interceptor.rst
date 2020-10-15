.. _query_interceptors:

Query Interceptors and Guards
=============================

GeoMesa provides a chance for custom logic to be applied to a query before executing it. Query interceptors must
be specified through user data in the simple feature type, and may be set before calling ``createSchema``, or
updated by calling ``updateSchema``. To indicate query interceptors, use the key ``geomesa.query.interceptors``:

.. code-block:: java

    sft.getUserData().put("geomesa.query.interceptors", "com.example.MyQueryInterceptor");

The value must be a comma-separated string consisting of the names of one or more classes implementing
the trait ``org.locationtech.geomesa.index.planning.QueryInterceptor``:

.. code-block:: scala

    /**
      * Provides a hook to modify a query before executing it
      */
    trait QueryInterceptor extends Closeable {

      /**
        * Called exactly once after the interceptor is instantiated
        *
        * @param ds data store
        * @param sft simple feature type
        */
      def init(ds: DataStore, sft: SimpleFeatureType): Unit

      /**
        * Modifies the query in place
        *
        * @param query query
        */
      def rewrite(query: Query): Unit

      /**
       * Hook to allow interception of a query after extracting the query values
       *
       * @param strategy query strategy
       * @return an exception if the query should be stopped
       */
      def guard(strategy: QueryStrategy): Option[IllegalArgumentException] = None
    }

Interceptors must have a default, no-arg constructor. The interceptor lifecycle consists of:

1. The instance is instantiated via reflection, using its default constructor
#. The instance is initialized via the ``init`` method, passing in the data store containing the simple feature type
#. ``rewrite`` and ``guard`` are called once for each query
#. When the data store is disposed, the instance is cleaned up via the ``close`` method

Interceptors will be invoked in the order they are declared in the user data. In order to see detailed information
on the results of query interceptors, you can enable ``TRACE``-level logging on the class
``org.locationtech.geomesa.index.planning.QueryRunner$``.

Provided Query Guards
^^^^^^^^^^^^^^^^^^^^^

GeoMesa provides some basic query guards that will block overly broad queries (which can overwhelm the system).
For additional controls, see ``geomesa.query.timeout`` and ``geomesa.scan.block-full-table`` in
:ref:`geomesa_site_xml`.

Full Table Scan Query Guard
+++++++++++++++++++++++++++

The full table scan query guard will block queries which would cause a full table scan to be performed.
This query guard is provided so that per feature type configuration can be managed easily.  Alternatively, one can use the
``geomesa.scan.block-full-table`` system property to disable full table scans across all feature types.

Just like the ``geomesa.scan.block-full-table`` property, this guard respects the ``geomesa.scan.block-full-table.threshold``
system property.  This allows for preview queries which can be helpful to show a system is working.

To enable the guard, add ``org.locationtech.geomesa.index.planning.guard.FullTableScanQueryGuard``
to ``geomesa.query.interceptors`` as indicated above.

.. code-block:: java

    sft.getUserData().put("geomesa.query.interceptors",
      "org.locationtech.geomesa.index.planning.guard.FullTableScanQueryGuard");


Temporal Query Guard
++++++++++++++++++++

The temporal query guard will block queries which exceed a maximum temporal duration.
Any query which attempts to return a larger time period will be stopped.
This guard also applies the full table scan guard.
The temporal query guard will not affect queries which do not execute a full table scan against indices that do not have
a temporal component (for example, feature ID and attribute queries).

To enable the guard, add ``org.locationtech.geomesa.index.planning.guard.TemporalQueryGuard``
to ``geomesa.query.interceptors`` as indicated above, and set the duration using ``geomesa.guard.temporal.max.duration``:

.. code-block:: java

    sft.getUserData().put("geomesa.query.interceptors",
      "org.locationtech.geomesa.index.planning.guard.TemporalQueryGuard");
    sft.getUserData().put("geomesa.guard.temporal.max.duration", "28 days");

Graduated Query Guard
+++++++++++++++++++++

The graduated query guard applies different duration limits based on the spatial extent of the query.
As a query becomes larger in space, it can be limited to shorter and shorter time ranges.
A series of rules limit the duration for queries which are at most a given size in square degrees.
This guard also applies the full table scan guard.  

To enable the guard, add ``org.locationtech.geomesa.index.planning.guard.GraduatedQueryGuard``
to ``geomesa.query.interceptors`` as indicated above.  Configuration is managed via
`TypeSafe Config <https://github.com/lightbend/config>`_ which will look for files named
``reference.conf`` and/or ``application.conf`` on the classpath.
For use in GeoServer, a file name ``reference.conf`` can be added to ``WEB-INF/classes``.
The configuration is under the key ``geomesa.guard.graduated``.

The configuration must satisfy a few conditions:

* there must be a limit on unbounded queries,
* as the size increases, the duration must decrease,
* and a given size limit may not be repeated.

An example is given here.  Durations can be given in a number of days, hours, or minutes.

.. code-block:: none

    geomesa {
      guard {
        graduated {
          "sftName" = [
            { size = 1,  duration = "60 days" }
            { size = 10, duration = "3 days"  }
            { duration = "1 day" }
          ]
        }
      }
    }
