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

Feature expiration is based on HBase's time-to-live functionality. See https://hbase.apache.org/book.html#ttl for details.

Note that because the expiration is an active process, expired features may still be returned until the
expiration process purges them. If precise expiration is required, use attribute-based expiration and apply a
default CQL filter for each query. For example, in GeoServer you can set a default layer filter to something like
``dtg > currentDate('-P1D')``. See :ref:`filter_functions` for an explanation of the ``currentDate`` function.
