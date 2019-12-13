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
    sft.getUserData().put("geomesa.table.compression.enabled", "true");
    sft.getUserData().put("geomesa.table.compression.type", "snappy");

.. code-block:: shell

    geomesa-hbase create-schema --compression snappy ...

For more information on how to set schema options, see :ref:`set_sft_options`.
