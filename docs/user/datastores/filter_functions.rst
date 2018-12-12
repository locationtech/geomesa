.. _filter_functions:

Filter Functions
================

GeoMesa provides several custom CQL filter functions, which can be used for filtering or transforming query results.
Filter functions can be created through ``ECQL.toFilter``, or directly through ``FilterFactory2.function``.

currentDate
-----------

Returns the current date as a ``java.util.Date``. Accepts an optional offset, specified as an ISO-8601 duration
string. See the Oracle
`Javadoc <https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence->`__ for
details on duration parsing.

dateToLong
----------

Converts a ``java.util.Date`` to a ``Long`` representing milliseconds since the epoch, as returned by
``date.getTime()``.

jsonPath
--------

See :ref:`json_path_filter_function`.

proxyId
-------

Converts the current ``FeatureId`` to an ``Int``. This may be used to minimize data size, while providing a unique
lookup mechanism. In particular, when returning data using :ref:`arrow_encoding`, the feature ID can be proxied
down to four bytes. When retrieving the full feature, the spatio-temporal values can be used to provide a fast
lookup, and the proxy ID can be used for disambiguation.

visibility
----------

Evaluates the current user's authorizations against a feature's visibility markings. Accepts an optional
parameter to specify the feature attribute that contains the visibility marking, otherwise will use the
default user-data location.
