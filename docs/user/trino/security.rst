.. _trino_security:

Trino Data Store Security
=========================

Row Entitlements
----------------

Tables may carry a per-row visibility expression in a ``__vis__`` column (the
column the FileSystem data store populates); NULL/empty rows are unrestricted.
A bare column merely named ``visibilities`` is treated as ordinary user data,
not an enforcement column. For any
table with a visibility column, the data store builds an ``AuthorizationsProvider``
(geomesa-security SPI) and ANDs an ``is_visible`` UDF predicate into every read,
count, and bounds query so that filtering runs inside Trino workers and counts stay
correct. Enforcement is **fail-closed**: with no ``geomesa.security.*`` parameters
configured, the provider grants no authorizations and only unrestricted rows are
returned.

.. list-table::
    :header-rows: 1
    :widths: 35 10 55

    * - Parameter
      - Required
      - Description
    * - ``geomesa.security.auths``
      - no
      - Comma-delimited superset of authorizations to be used for queries
    * - ``geomesa.security.auths.force-empty``
      - no
      - Don't use implicit authorizations from the underlying Trino user
    * - ``geomesa.security.auths.provider``
      - no
      - Explicit ``AuthorizationsProvider`` instance

Trino-Layer Enforcement for Direct-SQL Consumers
------------------------------------------------

Direct Trino SQL / JDBC / BI consumers are filtered separately by the spatial
plugin's connector access control, configured as **catalog** properties on
``spatial_iceberg`` (not data store parameters):

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

Setting either property opts the catalog into Trino-layer enforcement.

.. warning::

    Only the ``spatial_iceberg`` catalog is protected — do not expose a plain
    ``iceberg`` catalog over the same tables to untrusted users.
