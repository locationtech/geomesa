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
  authorizations can only ever see rows with a NULL visibility, so files that contain no NULLs
  are pruned. This tier is sound for any visibility grammar and needs no configuration.

* **Expression tier** — active when ``geomesa.security.visibility-expressions`` is non-empty.
  Each declared value is evaluated through the same ``is_visible()`` decision the row filter
  uses, so files whose visibility values are not admissible for the user are pruned. This tier
  correctly handles compound (``&`` / ``|``) visibility expressions.

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

