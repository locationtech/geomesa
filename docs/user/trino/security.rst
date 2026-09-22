.. _trino_security:

Trino Data Store Security
=========================

The GeoMesa Trino store supports :ref:`data_security`. Authorizations are enforced as an injected ``WHERE`` clause, based
on the user's access.

Trino-Layer Enforcement for Direct-SQL Consumers
------------------------------------------------

Direct Trino consumers are filtered separately by the spatial plugin's connector access control, configured as catalog
properties in Trino (not data store parameters):

.. list-table::
    :header-rows: 1
    :widths: 35 10 55

    * - Catalog property
      - Required
      - Description
    * - ``geomesa.security.auth-resolver``
      - no
      - ``file`` (default) or a fully-qualified ``AuthorizationResolver`` class for an external lookup
    * - ``geomesa.security.auth-mapping-file``
      - with ``file``
      - Path to a properties file mapping ``user.<n>`` / ``group.<n>`` → comma-delimited auth tokens
    * - ``geomesa.security.auths-secret``
      - no
      - Shared secret that clients must present in order to access the catalog

Setting either property opts the catalog into Trino-layer enforcement.

.. warning::

    Only the ``spatial_iceberg`` catalog is protected — do not expose a plain
    ``iceberg`` catalog over the same tables to untrusted users.

.. _trino_view_security:

Views
-----

Views are supported under Trino-layer enforcement, but they shift where auth enforcement happens, which carries
important requirements that an administrator must consider. A view is resolved through the connector's view metadata
and its body is expanded into base-table scans; the view relation itself is not a table and carries no ``__vis__``
column of its own. The connector therefore treats a view as follows:

* If the view projects a ``__vis__`` column through to its output, that column is filtered on the querying user's auth
  tokens, exactly as it would be for a table.
* If it does not, the view relation is left unfiltered and enforcement falls entirely to the scans its body expands
  to — which **delegates enforcement to the underlying catalog**.

Auths applied when querying a view
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino views default to ``DEFINER`` security, meaning the base tables are read as the view *creator/owner*.
Under row-visibility enforcement the injected ``WHERE`` clause would be built from the owner's auth tokens, and any
user permitted to query the view would see the owner's filtered rows — possible auth escalation/spillage.

To prevent that, views in the ``spatial_iceberg`` catalog are rewritten to run as the invoker, so every base-table scan
resolves the auth tokens of the user actually running the query. This is on by default and is controlled by
``geomesa.security.use-invoker-auths`` (see :ref:`trino_use_invoker_auths`).

.. warning::

    The rewrite applies only to views *defined in* the ``spatial_iceberg`` catalog, which is geomesa-trino's enforcement
    point.

Views over other connectors
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``spatial_iceberg`` access control **can only filter rows in its own iceberg catalog**. It has no ability to filter
a table read from another catalog via a view defined in ``spatial_iceberg``. Creating any view that joins, unions, or
otherwise reads a non-``spatial_iceberg`` table (i.e. via PostGIS, PostgreSQL, Hive) **will return rows unfiltered by
this connector**, no matter how the ``geomesa.security.*`` properties are set.

**This creates an implicit requirement:** Once row-visibility enforcement is enabled on ``spatial_iceberg``, **any
catalog reachable through a view must enforce its own row-level visibility filtering**, resolved for the same
querying user. For example, a trino connector pointing at PostGIS must be configured (and PostGIS configured) to
ensure visibility filtering (see :ref:`data_security`). **A catalog that does not enforce visibility filtering must not
be attached to a view on the spatial_iceberg connector that is intended to perform visibility filtering.**
