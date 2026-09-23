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

.. _trino_visibility_pruning:

Visibility-Column File Pruning
------------------------------

When Trino-layer enforcement is configured (see above), the ``spatial_iceberg`` connector can
additionally use Iceberg per-file manifest statistics to skip whole data files that cannot
contain any row the querying user is authorized to see. This is a **performance optimization
only**: it runs alongside — never in place of — the always-enforced ``is_visible()`` row filter,
so it can never cause a hidden row to be returned. It is opt-in, and controlled by these catalog
properties:

.. list-table::
    :header-rows: 1
    :widths: 35 10 55

    * - Catalog property
      - Required
      - Description
    * - ``geomesa.security.enable-visibility-expression-pruning``
      - no
      - Master switch for visibility-column file pruning (default ``false``). When unset or
        ``false``, no pruning is attempted and behavior is identical to enforcement without this
        feature. Requires ``geomesa.security.auth-resolver`` to be configured.
    * - ``geomesa.security.visibility-expressions``
      - no
      - Comma-separated list declaring every distinct non-null visibility value the visibility
        column can hold (e.g. ``basic,basic&privileged``). Enables the expression-pruning tier;
        when omitted, only the empty-authorizations tier is active.

The feature has two tiers:

* **Empty-authorizations tier** — active whenever pruning is enabled. A user with no
  authorizations can satisfy no visibility expression, and NULL/empty visibilities are hidden
  from everyone (see the note below), so such a user can see no rows at all and every file is
  pruned. This tier is sound for any visibility grammar and needs no configuration.

* **Expression tier** — active when ``geomesa.security.visibility-expressions`` is non-empty.
  Each declared value is evaluated through the same ``is_visible()`` decision the row filter
  uses, so files whose visibility values are not admissible for the user are pruned. This tier
  correctly handles compound (``&`` / ``|``) visibility expressions.

.. note::

    A NULL or empty (``''``) ``__vis__`` value carries no real visibility expression. The
    ``spatial_iceberg`` connector treats such a value as an anomaly and hides the row from
    **every** user (the ``is_visible()`` row filter returns false for it), and file pruning
    likewise never admits it. You therefore never need to declare ``''`` in
    ``geomesa.security.visibility-expressions``. This is intentionally stricter than the native
    geomesa-security ``VisibilityUtils`` semantics used by GeoTools clients, which treat
    NULL/empty as unrestricted.

.. warning::

    ``geomesa.security.visibility-expressions`` **must be complete** — it must list every distinct
    non-null visibility value that actually occurs in the data. A value that occurs in the data
    but is omitted from the list causes files holding only that value to be pruned, so a user
    whose authorizations *would* admit that value silently loses those rows (a correctness /
    availability issue). This is never a security leak — the ``is_visible()`` row filter still
    enforces confidentiality — but under-declaring prunes too much. Over-declaring is harmless: a
    declared value that never occurs simply never matches. When in doubt, declare more, not fewer.

    The list is catalog-wide; for a catalog whose tables use different visibility values, declare
    the union of all of them.

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
