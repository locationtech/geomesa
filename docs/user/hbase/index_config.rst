HBase Index Configuration
=========================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
This section contains HBase-specific options; general options can be found under :ref:`index_config`.

Setting File Compression
------------------------

You can enable HBase file compression when creating a new ``SimpleFeatureType`` by setting the appropriate
user data hints, or through the command line option. Valid compression types
are ``snappy``, ``lzo``, ``gz``, ``bzip2``, ``lz4`` or ``zstd``.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("geomesa.table.compression.type", "snappy");

.. code-block:: shell

    geomesa-hbase create-schema --compression snappy ...

For more information on how to set schema options, see :ref:`set_sft_options`.

Feature Expiration
------------------

HBase supports setting a per-feature time-to-live. Expiration can be set in the ``SimpleFeatureType`` user data,
using the key ``geomesa.feature.expiry``. See :ref:`set_sft_options` for details on configuring the user data.
Expiration can be set before calling ``createSchema``, or can be added to an existing schema by calling
``updateSchema``. However, note that if added through ``updateSchema``, any existing features will not be expired.

Feature expiration is based on HBase's time-to-live functionality. See https://hbase.apache.org/book.html#ttl for details.
