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
#. ``rewrite`` and ``guard`` are called once for each query (these two methods must be thread-safe)
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
The query guard will be loaded in all environments. Alternatively, one can use the ``geomesa.scan.block-full-table``
system property to disable full table scans per environment (see :ref:`geomesa_site_xml`).

Just like the ``geomesa.scan.block-full-table`` property, this guard respects the ``geomesa.scan.block-full-table.threshold``
system property.  This allows for preview queries which can be helpful to show a system is working.

To enable the guard, add ``org.locationtech.geomesa.index.planning.guard.FullTableScanQueryGuard``
to ``geomesa.query.interceptors`` as indicated above.

.. code-block:: java

    sft.getUserData().put("geomesa.query.interceptors",
      "org.locationtech.geomesa.index.planning.guard.FullTableScanQueryGuard");

To disable the guard on a per-environment basis, set the system property ``geomesa.scan.<typeName>.block-full-table``
to ``false``, where ``<typeName>`` is the name of your feature type.

Temporal Query Guard
++++++++++++++++++++

The temporal query guard will block queries which exceed a maximum temporal duration.
Any query which attempts to return a larger time period will be stopped.
The temporal query guard will not affect queries against indices that do not have
a temporal component (for example, feature ID and attribute queries).

To enable the guard, add ``org.locationtech.geomesa.index.planning.guard.TemporalQueryGuard``
to ``geomesa.query.interceptors`` as indicated above, and set the duration using ``geomesa.guard.temporal.max.duration``:

.. code-block:: java

    sft.getUserData().put("geomesa.query.interceptors",
      "org.locationtech.geomesa.index.planning.guard.TemporalQueryGuard");
    sft.getUserData().put("geomesa.guard.temporal.max.duration", "28 days");

To disable the guard on a per-environment basis, set the system property ``geomesa.guard.temporal.<typeName>.disable``
to ``true``, where ``<typeName>`` is the name of your feature type.

Graduated Query Guard
+++++++++++++++++++++

The graduated query guard applies different duration limits or result subsampling based on the spatial extent of the
query. As a query becomes larger in space, it can be limited to shorter and shorter time ranges or smaller and smaller
percentages of data. A series of rules limit the duration and percentage for queries which are at most a given size
in square degrees. This guard also applies the full table scan guard.

Percentage subsampling in this guard uses the same statistical sampling as :ref:`feature_sampling`.

To enable the guard, add ``org.locationtech.geomesa.index.planning.guard.GraduatedQueryGuard``
to ``geomesa.query.interceptors`` as indicated above.  Configuration is managed via
`TypeSafe Config <https://github.com/lightbend/config>`_ which will look for files named
``reference.conf`` and/or ``application.conf`` on the classpath.
For use in GeoServer, a file name ``reference.conf`` can be added to ``WEB-INF/classes``.
The configuration is under the key ``geomesa.guard.graduated``.

The configuration must satisfy a few conditions:

* there must be a limit on unbounded queries
* as the size increases, the duration must decrease
* as the size increases, the percentage must decrease
* once a duration or percentage is defined, all subsequent rules must also define it
* a given size limit may not be repeated

If no size is provided, it is equivalent to an unbounded size.

Durations can be given in a number of days, hours, or minutes. For example:

.. code-block:: none

    geomesa {
      guard {
        graduated {
          "sftName" = [
            { size = 1,  duration = "60 days" }
            { size = 10, duration = "3 days"  }
            {            duration = "1 day"   }
          ]
        }
      }
    }

Sampling percentages can be defined in decimal form. e.g. .1 corresponds to 10%. Any query smaller than the first size will
return 100% of the records. For example:

.. code-block:: none

    geomesa {
      guard {
        graduated {
          "sftName" = [
            { size = 1,  sampling-percentage = .8 }
            { size = 10, sampling-percentage = .5 }
            {            sampling-percentage = .1 }
          ]
        }
      }
    }

It is also possible to specify the sampling attribute to use for the threading key in subsampling:

.. code-block:: none

    geomesa {
      guard {
        graduated {
          "sftName" = [
            { size = 1,  sampling-percentage = .8, sampling-attribute = "name" }
            { size = 10, sampling-percentage = .5, sampling-attribute = "name" }
            {            sampling-percentage = .1, sampling-attribute = "name" }
          ]
        }
      }
    }

Additionally, it's possible to combine duration and percentage limits, with or without specifying an attribute:

.. code-block:: none

    geomesa {
      guard {
        graduated {
          "sftName" = [
            { size = 1,  duration = "60 days"                                                       }
            { size = 10, duration = "3 days", sampling-percentage = .5                              }
            {            duration = "1 day",  sampling-percentage = .1, sampling-attribute = "name" }
          ]
        }
      }
    }

In the above example, Any query with area less than 1 square degree will return all results and allow for any time
range (given the time range doesn't trigger a full table scan). A query larger than 1 but less than 10 will block any
query longer than 3 days AND only 50% of the results will be returned. Finally, any query larger than 10 must be less
than 1 day in length AND only 10% of the results for any "name" will be returned.

To disable the guard on a per-environment basis, set the system property ``geomesa.guard.graduated.<typeName>.disable``
to ``true``, where ``<typeName>`` is the name of your feature type.
