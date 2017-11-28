.. _cli_analytic:

Analytic Commands
=================

These commands are used to analyze data sets. Required parameters are indicated with a ``*``.

See :ref:`stat_attribute_config` for details on cached statistics.

``stats-bounds``
----------------

Display or calculate the bounds for the attributes of a schema.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL to filter the features to consider
``-a, --attributes``     Specific attributes to calculate bounds for
``--no-cache``           Don't use cached statistics
======================== =========================================================

By default, pre-computed (cached) bounds will be displayed. The cached bounds may not be
completely precise, but they are available instantly. If the ``--no-cache`` argument is used,
the bounds will be calculated by running a query against the data. This will give a precise
result, but take longer.

``stats-count``
---------------

Count features that match a predicate.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL filter to select features to export
``--no-cache``           Don't use cached statistics
======================== =========================================================

By default, pre-computed (cached) stats will be used to estimate the count. The cached stats may
not be completely precise, but they are available instantly. If the ``--no-cache`` argument is used,
the count will be calculated by running a query against the data. This will give a precise result,
but take longer.

``stats-histogram``
-------------------

Displays a histogram of the values of the given attributes.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL filter to select features to export
``-a, --attributes``     Specific attributes to calculate values for
``--bins``               Number of bins used to divide the histogram values
``--no-cache``           Don't use cached statistics
======================== =========================================================

The ``--bins`` argument will determine how the histogram is divided up. For example, when
examining a week of time data, using 7 bins would group values by day.

By default, pre-computed (cached) stats will be used to estimate histograms. The cached stats may
not be completely precise, but they are available instantly. If the ``--no-cache`` argument is used,
the histogram will be calculated by running a query against the data. This will give a precise result,
but take longer. Note that even with ``--no-cache``, summary statistical models are used to determine
histograms, and they may not be completely accurate.

Histograms on geometry-type attributes will displayed as rough heat maps.

``stats-top-k``
---------------

Displays the most common values for the given attributes.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL filter to select features to export
``-a, --attributes``     Specific attributes to calculate values for
``-k``                   Number of top values to show
``--no-cache``           Don't use cached statistics
======================== =========================================================

By default, pre-computed (cached) stats will be used to estimate the top values. The cached stats may
not be completely precise, but they are available instantly. If the ``--no-cache`` argument is used,
the top values will be calculated by running a query against the data. This will give a precise result,
but take longer. Note that even with ``--no-cache``, summary statistical models are used to determine
top values, and they may not be completely accurate.
