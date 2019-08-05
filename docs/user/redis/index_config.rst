Redis Index Configuration
=========================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
The Redis data store supports most of the general options described under :ref:`index_config`, with the exception of
column groups, attribute-level visibilities, sharding and table splitting.

.. _redis_feature_expiry:

Feature Expiration
------------------

Redis supports setting a per-feature time-to-live. Expiration can be set in the ``SimpleFeatureType`` user data,
using the key ``geomesa.feature.expiry``. See :ref:`set_sft_options` for details on configuring the user data.
Expiration can be set before calling ``createSchema``, or can be added to an existing schema by calling
``updateSchema``. However, note that if added through ``updateSchema``, any existing features will not be expired.

Expiration can be based on either ingest time or a feature attribute. To set expiration based on ingest time,
specify a time-to-live as a duration string, e.g. ``24 hours`` or ``180 days``. To set expiration based on
a feature attribute, specify the attribute along with a time-to-live in parentheses, e.g. ``dtg(24 hours)`` or
``event-time(30 days)`` (where ``dtg`` and ``event-time`` are ``Date``-type attributes in the schema).

Features are actively expired by a background process running in each data store instance. By default, features
will be expired every 10 minutes. This can be controlled by configuring the system property
``geomesa.redis.age.off.interval``. To disable the background process, the property can be set to ``Inf``. If
there are multiple active data store instances, they will synchronize among themselves to avoid duplicating work.

Note that because the expiration is an active process, expired features may still be returned until the
expiration process purges them. If precise expiration is required, use attribute-based expiration and apply a
default CQL filter for each query. For example, in GeoServer you can set a default layer filter to something like
``dtg > currentDate('-P1D')``. See :ref:`filter_functions` for an explanation of the ``currentDate`` function.
