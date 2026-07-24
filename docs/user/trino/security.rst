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
