.. _trino_runtime_configuration:

Trino Runtime Configuration
===========================

The Trino data store supports the following system properties:

``geomesa.trino.filter.client-side``
------------------------------------

``geomesa.trino.filter.client-side`` controls whether client-side filtering is supported or not. Client-side filters are
predicates that can't be evaluated directly by Trino, so have to be evaluated on the query results after they return to the
client. For example, a JSON-path predicate into a string-type field can't be translated into Trino SQL.

The possible values are:

* ``partial`` (default) - client-side filters are allowed as long as there is also a predicate that **can** be pushed down
* ``none`` - no client-side filters are allowed
* ``all`` - all client-side filters are allowed

Note that the max features for a query with client-side filters can't be pushed down, so queries may consume more resources
than expected.
