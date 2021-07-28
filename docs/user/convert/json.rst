.. _json_converter:

JSON Converter
==============

The JSON converter handles JSON files. To use the JSON converter, specify ``type = "json"`` in your converter
definition.

Configuration
-------------

The JSON converter supports parsing multiple JSON documents out of a single file.
In order to support JSON path expressions, each JSON document is fully parsed into memory.
For large documents, this may take considerable time and memory. Thus, it is usually better to have multiple
smaller JSON documents per file, when possible.

Since a single JSON document may contain multiple features, the JSON parser supports a
`JSONPath <https://goessner.net/articles/JsonPath/>`__ expression pointing to each feature element. This can
be specified using the ``feature-path`` element.

The ``fields`` element in a JSON converter supports two additional attributes, ``path`` and ``json-type``.
``path`` should be a `JSONPath <https://goessner.net/articles/JsonPath/>`__ expression, which is relative to the
``feature-path``, if defined (above). For absolute paths, ``root-path`` may be used instead of ``path``.
``json-type`` should specify the type of JSON field being read. Valid values are: **string**, **float**, **double**,
**integer**, **long**, **boolean**, **geometry**, **array** and **object**. The value will be appropriately typed,
and available in the ``transform`` element as ``$0``. Geometry types can handle either WKT strings or GeoJSON
geometry objects.

Handling Complex Elements
-------------------------

JSON can contain complex, nested elements that don't necessarily map well to the flat attribute structure used
by ``SimpleFeatureTypes``. These type of elements can be easily handled using GeoMesa's support for
:ref:`json_attributes`. In your ``SimpleFeatureType`` schema, indicate a complex JSON string through the
user data hint ``json=true``. In your converter, select the outer element and then turn it into a JSON string
through the ``toString`` transformer function. You will be able to filter and transform the data using JSONPath
at query time. See :ref:`json_attributes` for more details.

JSON Composite Converters
-------------------------

Composite converters can handle processing different JSON formats in a single stream. To use a composite
converter, specify ``type = "composite-json"`` in your converter definition.

Composite converters can define top-level options, fields, etc, the same as a normal JSON converter. These
values will be inherited by each of the child converters. If each child is unique, then it is valid to not
define anything at the top level.

Composite converters must define a ``converters`` element, which is an array of nested JSON converter
definitions. In addition to the standard configuration, each nested converter must have a ``predicate``
element that determines which converter to use for each JSON document. The value passed into the predicate
will be the parsed JSON document (available as ``$0``), so generally the predicate will make use of the
``jsonPath`` function (below). See :ref:`composite_predicates` for more details on predicates.

.. _json_converter_functions:

JSON Transform Functions
------------------------

The ``transform`` element supports referencing the JSON element through ``$0``. Each column will initially
be typed according to the field's ``json-type``. Most types will be converted to the equivalent Java class,
e.g. java.lang.Integer, etc. **array** and **object** types will be raw JSON elements, and thus usually
require further processing (e.g. ``jsonList`` or ``jsonMap``, below).

In addition to the standard functions in :ref:`converter_functions`, the JSON converter provides the following
JSON-specific functions:

emptyJsonToNull
~~~~~~~~~~~~~~~

This function converts empty JSON objects and arrays to null. A JSON object is also considered empty if all its
values are null.

jsonArrayToObject
~~~~~~~~~~~~~~~~~

This function converts a JSON array into a JSON object, by using the index of each array element as the object
key. This is useful for GeoMesa's JSON attribute types, which currently require a top-level object and not an array.

jsonList
~~~~~~~~

This function converts a JSON array element into a java.util.List. It requires two parameters; the first is the
type of the list elements as a string, and the second is a JSON array. The type of list elements must be one
of the types defined in :ref:`attribute_types`. See below for an example.

jsonMap
~~~~~~~

This function converts a JSON object element into a java.util.Map. It requires three parameters; the first is the
type of the map key elements as a string, the second is the type of the map value elements as a string, and the
third is a JSON object. The type of keys and values must be one of the types defined in :ref:`attribute_types`.
See below for an example.

<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
mapToJson
~~~~~~~~~

This function converts a java.util.Map into a JSON string. It requires a single parameter, which must be a
java.util.Map. It can be useful for storing complex JSON as a single attribute, which can then be queried
using GeoMesa's JSON attribute support. See :ref:`json_attributes` for more information.

jsonArrayToObject
~~~~~~~~~~~~~~~~~

This function converts a JSON array into a JSON object, by using the index of each array element as the object
key. This is useful for GeoMesa's JSON attribute types, which currently require a top-level object and not an array.

>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
jsonPath
~~~~~~~~

This function will evaluate a `JSONPath <https://goessner.net/articles/JsonPath/>`__ expression against a
given JSON element. Generally, it is better to use the ``path`` element of the ``fields`` element, but
this method can be useful for composite predicates (see above). The first argument is the path to evaluate,
and the second argument is the element to operate on.

mapToJson
~~~~~~~~~

This function converts a java.util.Map into a JSON string. It requires a single parameter, which must be a
java.util.Map. It can be useful for storing complex JSON as a single attribute, which can then be queried
using GeoMesa's JSON attribute support. See :ref:`json_attributes` for more information.

<<<<<<< HEAD
newJsonObject
~~~~~~~~~~~~~
=======
jsonArrayToObject
~~~~~~~~~~~~~~~~~

This function converts a JSON array into a JSON object, by using the index of each array element as the object
key. This is useful for GeoMesa's JSON attribute types, which currently require a top-level object and not an array.

=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
jsonPath
~~~~~~~~
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))

This function creates a new JSON object from key-value pairs. It can be useful for generating JSON text values.

Example::

  fields = [
    { name = "foo", path = "$.foo", json-type = "String" }
    { name = "bar", path = "$.bar", json-type = "Array" }
    { name = "foobar", transform = "toString(newJsonObject('foo', $foo, 'bar', $bar))"
  ]

mapToJson
~~~~~~~~~

This function converts a java.util.Map into a JSON string. It requires a single parameter, which must be a
java.util.Map. It can be useful for storing complex JSON as a single attribute, which can then be queried
using GeoMesa's JSON attribute support. See :ref:`json_attributes` for more information.

newJsonObject
~~~~~~~~~~~~~

This function creates a new JSON object from key-value pairs. It can be useful for generating JSON text values.

Example::

  fields = [
    { name = "foo", path = "$.foo", json-type = "String" }
    { name = "bar", path = "$.bar", json-type = "Array" }
    { name = "foobar", transform = "toString(newJsonObject('foo', $foo, 'bar', $bar))"
  ]

Example Usage
-------------

Assume the following SimpleFeatureType:

::

  geomesa.sfts.example = {
    attributes = [
      { name = "name",    type = "String"          }
      { name = "age",     type = "Integer"         }
      { name = "weight",  type = "Double"          }
      { name = "hobbies", type = "List[String]"    }
      { name = "skills",  type = "Map[String,Int]" }
      { name = "source",  type = "String"          }
      { name = "geom",    type = "Point"           }
    ]
  }

And the following JSON document:

::

  {
    "DataSource": { "name": "myjson" },
    "Features": [
      {
        "id": 1,
        "name": "phil",
        "physicals": {
          "age": 32,
          "weight": 150.2
        },
        "hobbies": [ "baseball", "soccer" ],
        "languages": {
          "java": 100,
          "scala": 70
        },
        "geometry": { "type": "Point", "coordinates": [55, 56] }
      },
      {
        "id": 2,
        "name": "fred",
        "physicals": {
          "age": 33,
          "weight": 150.1
        },
        "hobbies": [ "archery", "tennis" ],
        "languages": {
          "c++": 10,
          "fortran": 50
        },
        "geometry": { "type": "Point", "coordinates": [45, 46] }
      }
    ]
  }

You could ingest with the following converter:

::

 geomesa.converters.myjson = {
   type         = "json"
   id-field     = "$id"
   feature-path = "$.Features[*]"
   fields = [
     { name = "id",      json-type = "integer",  path = "$.id",               transform = "toString($0)"                }
     { name = "name",    json-type = "string",   path = "$.name",             transform = "trim($0)"                    }
     { name = "age",     json-type = "integer",  path = "$.physicals.age",                                              }
     { name = "weight",  json-type = "double",   path = "$.physicals.weight"                                            }
     { name = "hobbies", json-type = "array",    path = "$.hobbies",          transform = "jsonList('string', $0)"      }
     { name = "skills",  json-type = "map",      path = "$.languages",        transform = "jsonMap('string','int', $0)" }
     { name = "geom",    json-type = "geometry", path = "$.geometry",         transform = "point($0)"                   }
     { name = "source",  json-type = "string",   root-path = "$.DataSource.name"                                        }
   ]
 }
