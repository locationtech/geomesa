.. _converters:

GeoMesa Convert
===============

GeoMesa Convert is a configurable and extensible library for converting data into GeoTools ``SimpleFeature``\ s,
which can then be written to GeoMesa. The convert library is the easiest way to ingest data into
GeoMesa, and ships with the GeoMesa command-line tools. Data types are defined in the JSON-like
`HOCON <https://github.com/lightbend/config/blob/master/HOCON.md>`__ format, which allows for ingesting a wide
variety of data without any custom code.

Converter definitions are provided out-of-the-box for some common open-source data sets, including Twitter and
GDELT; see :ref:`prepackaged_converters` for details. In addition, common file formats such as GeoJSON,
delimited text, or self-describing Avro can often be ingested without a converter. See :ref:`cli_ingest` for
details.

.. toctree::
    :maxdepth: 1

    common
    parsing_and_validation
    delimited_text
    json
    xml
    avro
    avro_schema_registry
    shp
    fixed_width
    jdbc
    composite
    cache
    premade/index
    function_overview
    function_usage
    usage_tools
    usage_runtime
    extending
