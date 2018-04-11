.. _converter_common:

Converter Basics
================

Converters and SimpleFeatureTypes are defined as `HOCON <https://github.com/lightbend/config/blob/master/HOCON.md>`__
files. GeoMesa uses the `TypeSafe Config <https://github.com/typesafehub/config>`__ library to load the configuration
files. In effect, this means that converters should be defined in a file called ``application.conf`` and placed at
the root of the classpath. In the GeoMesa tools distribution, the files can be placed in the ``conf`` folder. See
`Standard Behavior <https://github.com/lightbend/config#standard-behavior>`__ for more information on how
TypeSafe loads files.

Defining SimpleFeatureTypes
---------------------------

In GeoTools, a SimpleFeatureType defines the schema for your data. It is similar to defining a SQL database table,
as it consists of strongly-typed, ordered, named attributes (columns). The converter library supports
SimpleFeatureTypes defined in `HOCON <https://github.com/lightbend/config/blob/master/HOCON.md>`__.
SimpleFeatureTypes should be written as objects under the path ``geomesa.sfts``.

The name for the SimpleFeatureType will be the name of the HOCON element (e.g. 'example', below), or it can
be overridden with ``type-name``.

A SimpleFeatureType definition consists of an ``attributes`` array, and an optional ``user-data`` section.

``attributes`` is an array of column definitions, each of which must include a ``name`` and a ``type``.
See :ref:`attribute_types` for supported types. See :ref:`reserved-words` for names that aren't supported.
Any additional keys beyond those two will be set as user data, and can be used to configure various
attribute-level options.

The ``user-data`` element consists of key-value pairs that will be set in the user data for the SimpleFeatureType.
This can be used to configure various schema-level options.

See :ref:`index_config` for details on the configuration options available.

Example::

  geomesa = {
    sfts = {
      example = {
        type-name = "example"
        attributes = [
          { name = "name", type = "String", index = true }
          { name = "age", type = "Integer" }
          { name = "dtg", type = "Date", default = true }
          { name = "geom", type = "Point", default = true, srid = 4326 }
        ]
        user-data = {
          option.one = "value"
        }
      }
    }
  }

This example is equivalent to the following specification string:

.. code:: scala

  SimpleFeatureTypes.createType("example",
      "name:String:index=true,age:Integer,dtg:Date:default=true,*geom:Point:srid=4326;option.one='value'")

Defining Converters
-------------------

A converter defines the mapping between source data (CSV, JSON, XML, etc) and a SimpleFeatureType. The converter
accepts as input source files, and outputs GeoTools SimpleFeatures, which can then be written to GeoMesa.
Thus, each converter corresponds to a single SimpleFeatureType, although there may be multiple converters for each
SimpleFeatureType. The converter library supports converters defined in
`HOCON <https://github.com/lightbend/config/blob/master/HOCON.md>`__. Converters should be written as objects under
the path ``geomesa.converters``.

Converters are generally defined with a ``type`` and a ``fields`` array. Optionally,
they may define an ``id-field``, ``user-data`` and configuration ``options``.

The ``type`` element specifies the type of the converter, for example 'delimited-text' or 'json'. Specific converters
will have additional options that are not covered here. See :ref:`converters` for more information on the types available.

The ``fields`` array defines the attributes created by the converter. Each field consists of a ``name`` and
an optional ``transform``. Specific converters support additional field options; see the documentation
on each converter type for details.

If the ``name`` of a field corresponds with the ``name`` of a SimpleFeatureType attribute, then it will be set as
that attribute when converting to SimpleFeatures. Intermediate fields may be defined in order to build up
complex attributes, and can be referenced by name in other fields.

The ``transform`` of a field can be used to reference other fields or modify the raw value extracted from the
source data. Other fields can be referenced by name using ``$`` notation; for example, ``$age`` references the
field named 'age'. Transforms can also include function calls. GeoMesa includes a variety of useful transform
functions, and supports loading custom functions from the classpath. See :ref:`converter_functions` for details.

The ``id-field`` element will set the feature ID for the SimpleFeature. It accepts any values that would normally
be in a field ``transform``, so it can reference other fields and call transform functions. A common pattern
is to use a hash of the entire input record for the ``id-field``; that way the feature ID is consistent if the
same data is ingested multiple times. If the ``id-field`` is omitted, GeoMesa will generate random UUIDs for
each feature.

The ``user-data`` element supports arbitrary key-value pairs that will be set in the user data for each SimpleFeature.
For example, it could be used to specify feature-level :ref:`accumulo_visibilities`.

The ``options`` element supports parsing and validation behavior. See :ref:`converter_validation` for details.

