.. _converters:

GeoMesa Convert
===============

A configurable and extensible library for converting data into
``SimpleFeature``\ s, found in ``geomesa-convert`` in the source distribution.

Converters for various different data formats can be configured and
instantiated using the ``SimpleFeatureConverters`` factory and a target
``SimpleFeatureType``. Converters are currently available for these formats:

-  delimited text (CSV, TSV)
-  fixed width text
-  :ref:`Avro <avro_converter>`
-  :ref:`JSON <json_converter>`
-  :ref:`XML <xml_converter>`
-  :ref:`JDBC <jdbc_converter>`

The converter allows the specification of fields extracted from the data
and transformations on those fields. Syntax of transformations is very
much like ``awk`` syntax. Fields with names that correspond to attribute
names in the ``SimpleFeatureType`` will be directly populated in the
result SimpleFeature. Fields that do not align with attributes in the
``SimpleFeatureType`` are assumed to be intermediate fields used for
deriving attributes. Fields can reference other fields by name for
building up complex attributes.

``SimpleFeatureType``\ s and converters are specified as HOCON files readable
by the `Typesafe Config`_ library.

.. _Typesafe Config: https://github.com/typesafehub/config

.. toctree::
    :maxdepth: 1

    example
    usage_tools
    usage_runtime
    premade/index
    function_overview
    function_usage
    json
    xml
    avro
    jdbc
    extending
    parsing_and_validation
    cache