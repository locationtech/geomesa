.. _converter_common:

Converter Basics
================

Converters and SimpleFeatureTypes are defined as `HOCON <https://github.com/lightbend/config/blob/master/HOCON.md>`__
files. GeoMesa uses the `TypeSafe Config <https://github.com/typesafehub/config>`__ library to load the configuration
files. In effect, this means that converters should be defined in a file called ``application.conf`` and placed at
the root of the classpath. In the GeoMesa tools distribution, the files can be placed in the ``conf`` folder. See
`Standard Behavior <https://github.com/lightbend/config#standard-behavior>`__ for more information on how
TypeSafe loads files.

.. _converter_sft_defs:

Defining SimpleFeatureTypes
---------------------------

In GeoTools, a SimpleFeatureType defines the schema for your data. It is similar to defining a SQL database table,
as it consists of strongly-typed, ordered, named attributes (columns). The converter library supports
SimpleFeatureTypes defined in `HOCON <https://github.com/lightbend/config/blob/master/HOCON.md>`__.
SimpleFeatureTypes should be written as objects under the path ``geomesa.sfts``.

The following keys are supported:

``type-name``
^^^^^^^^^^^^^

``type-name`` is an optional string used to specify the name for the SimpleFeatureType. If not included, the name of the
SimpleFeatureType will be the name of the HOCON element (e.g. 'example', below).

``attributes``
^^^^^^^^^^^^^^

``attributes`` is an array of column definitions, each item of which supports the following keys:

=============== ======== ======= =================================================================================================
Key             Required Type    Description
=============== ======== ======= =================================================================================================
``name``        yes      String  The name of the attribute. See :ref:`reserved-words` for names that aren't supported.
``type``        yes      String  The type of the attribute. See :ref:`attribute_types` for supported types.
``default``     no       Boolean Indicates the default date and/or geometry field. At most one date and/or one geometry field may
                                 be marked as default. Has no effect for other attribute types.
``index``       no       String  Creates an index on the attribute. See :ref:`attribute_indices` for details.
``cardinality`` no       String  Adds a hint used for query planning. See :ref:`attribute_cardinality` for details.
``srid``        no       Integer The coordinate reference system, for Geometry-type attributes. Note that GeoMesa currently only
                                 supports ``srid:4326``, i.e. latitude/longitude.
``json``        no       Boolean Indicates that the attribute is a JSON-encoded object. See :ref:`json_attributes`.
=============== ======== ======= =================================================================================================

The above keys are the most commonly used, but see :ref:`index_config` for details on all of the available options, which
include things like column groups, numeric precision, and cached statistics.

``user-data``
^^^^^^^^^^^^^

``user-data`` is an optional object that consists of key-value pairs representing user data for the SimpleFeatureType.
This can be used to configure various schema-level options. See :ref:`index_config` for details on configuration options.

Example
^^^^^^^

::

  geomesa.sfts.example = {
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

This example is equivalent to the following specification string:

.. code:: scala

  SimpleFeatureTypes.createType("example",
      "name:String:index=true,age:Integer,dtg:Date:default=true,*geom:Point:srid=4326;option.one='value'")

Defining Converters
-------------------

A converter defines the mapping between source data (CSV, JSON, XML, etc) and a SimpleFeatureType. The converter
transforms a data stream or file into GeoTools SimpleFeatures, which can then be written to GeoMesa. The converter library
supports converters defined in `HOCON <https://github.com/lightbend/config/blob/master/HOCON.md>`__. Converters should be
written as objects under the path ``geomesa.converters``.

Although specific converters may have additional options, the following common keys are supported:

``type``
^^^^^^^^

``type`` is a required key that defines the converter that will be used, such as ``json``, ``xml`` or ``avro``. See
:ref:`converters` for a list of the available types.

``fields``
^^^^^^^^^^

``fields`` is a required key that defines an array of field definitions. Specific converters support additional field options,
but all fields support the following keys:

=============== ======== ======= =================================================================================================
Key             Required Type    Description
=============== ======== ======= =================================================================================================
``name``        yes      String  The name of the field. If the name corresponds to an attribute in the SimpleFeatureType, it will
                                 be used to populate that attribute.
``transform``   no       String  Modifies the value of the field. Transforms support a simple syntax, where other fields can be
                                 referenced by name using ``${}`` notation, and functions can be called by name with their
                                 arguments in parenthesis ``()``. See :ref:`converter_functions` for a list of available
                                 functions.
=============== ======== ======= =================================================================================================

Intermediate fields (fields where the ``name`` does not correspond to an attribute) may be defined in order to build up
complex attributes, and can be referenced by name in other fields.

``id-field``
^^^^^^^^^^^^

``id-field`` is an optional key that sets the feature ID for a SimpleFeature. It accepts any values that would normally be in a
field ``transform``, so it can reference other fields and call transform functions. A common pattern is to use a hash of the
entire input record for the ``id-field``; that way the feature ID is consistent if the same data is ingested multiple times.
If the ``id-field`` is omitted, GeoMesa will generate a random UUID for each feature.

``options``
^^^^^^^^^^^

``options`` is an optional key for customizing parsing and validation behavior. See :ref:`converter_validation` for details.
In addition, specific converters may support additional option keys, which are described in the documentation for
each converter type.

``user-data``
^^^^^^^^^^^^^

``user-data`` is an optional key for an object used to set feature-level user data. The main use case is for :ref:`data_security`.

``caches``

``caches`` is an optional key that supports data enrichment from external sources. See :ref:`converter_caches` for details.

.. _converter_metrics:

Metrics
-------

Converters use the `Micrometer <https://docs.micrometer.io/micrometer/reference/>`__ library to register metrics for
successful conversions, failed conversions, validation errors, and processing rates. See :ref:`geomesa_metrics` for details
on exposing metrics through registries.
