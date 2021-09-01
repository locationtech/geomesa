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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c296484bff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0fb40a8655 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2e820cbfbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a42015a87c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d6e3d58599 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ec072ceda6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> cc6506f342 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0154226e18 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9f616db357 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8169a66b76 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 344200f1df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 70f268c58f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69016fbbb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4e989397f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d73ae82492 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 86c0805e1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95d2c10f10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eeb8ab478a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 805eebdbb2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d58a385c84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c9bb37cf42 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75810048c9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afcf525d7d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92078ab70d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b44c2cac84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cdb062d5e9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 775feff6dd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 38f2492609 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cb3028faf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b515d530a4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ba177f9f13 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d7cc16ab (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2a68a987a1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df9cd4f22d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64c0971d8e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b515d530a4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ba177f9f13 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1d4e479464 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de00bd1943 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ea5f19f56b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54fb546133 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e0056031ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9888cda97e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5f0745e34b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 29dcc37810 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69a4e317d6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> be1167e028 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fc179cb2d5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e74e1d9b4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f98490578 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1417ba30dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6bef55a1ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ce7c794944 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19108609dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f1f448d9e0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3adb85864f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 49d3846b23 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ea5f19f56b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3adb85864f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 49d3846b23 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72ff05570e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b0a7faa6b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94096ec6bd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d98e354b8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a52516cf49 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1351a5baa4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4002ece69 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7273b06d00 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cee8a0fb22 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 857489f8a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 083bb9617b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 32aba52002 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c630f9360 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b69b6b3ea1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 665046a461 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65c5b3f5d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbc20a86a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b1dea55b93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d3ca180ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 73d82b731c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c673847c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ebbfa035a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad7a284ebe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3fe983979 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d539392ecd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d9ec67b278 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4010468d90 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54fb546133 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e0056031ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9888cda97e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5f0745e34b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 29dcc37810 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69a4e317d6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3adb85864f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 49d3846b23 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 7d705cac10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 657d996e28 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33dcb1f603 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64c4b371aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8354d11583 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 46b82ddad2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8b0bfd55f9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8e7c6661dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d83ac69cc5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2adb521281 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 307fc2b238 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e74e1d9b4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d9ec67b278 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ea5f19f56b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ce7c794944 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e74e1d9b4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d9ec67b278 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e74e1d9b4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d9ec67b278 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e74e1d9b4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d9ec67b278 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3f98490578 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 4e989397f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d73ae82492 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86c0805e1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> ad7a284ebe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad7a284ebe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d73ae82492 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9888cda97e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> ad7a284ebe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 69016fbbb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4e989397f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d73ae82492 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 86c0805e1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92078ab70d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92078ab70d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc6506f342 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92078ab70d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> 92078ab70d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad7a284ebe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4e989397f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 86c0805e1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 75810048c9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75810048c9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75810048c9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e81892dbf2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3f98490578 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea5f19f56b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ce7c794944 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 19108609dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 77f1dd8356 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9888cda97e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e74e1d9b4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9e74e1d9b4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ea5f19f56b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 6bc7d58545 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aa5808a811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 657d996e28 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33dcb1f603 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c4b371aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8354d11583 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ec072ceda6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1351a5baa4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc20a86a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 46b82ddad2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 8b0bfd55f9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8e7c6661dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dbc20a86a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d83ac69cc5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0fb40a8655 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b0a7faa6b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d98e354b8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cee8a0fb22 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2adb521281 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7d705cac10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0fb40a8655 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2e820cbfbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ec072ceda6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 72ff05570e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3b0a7faa6b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d98e354b8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1351a5baa4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b4002ece69 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7273b06d00 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cee8a0fb22 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 857489f8a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 083bb9617b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 32aba52002 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2c630f9360 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 665046a461 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 65c5b3f5d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> dbc20a86a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7d3ca180ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 73d82b731c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c673847c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ad7a284ebe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d9ec67b278 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d9ec67b278 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f98490578 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ce7c794944 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19108609dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2a68a987a1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2a68a987a1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc6506f342 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 69016fbbb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4e989397f2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 69016fbbb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d73ae82492 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4e989397f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 86c0805e1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9888cda97e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d73ae82492 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 86c0805e1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95d2c10f10 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 95d2c10f10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d58a385c84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d58a385c84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75810048c9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 75810048c9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b44c2cac84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b44c2cac84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 38f2492609 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f2492609 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f1f448d9e0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d7cc16ab (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2a68a987a1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df9cd4f22d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64c0971d8e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc6506f342 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0154226e18 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9f616db357 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8169a66b76 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 344200f1df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 70f268c58f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69016fbbb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4e989397f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d73ae82492 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 86c0805e1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95d2c10f10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eeb8ab478a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 805eebdbb2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d58a385c84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c9bb37cf42 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75810048c9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afcf525d7d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92078ab70d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b44c2cac84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cdb062d5e9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 775feff6dd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 38f2492609 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cb3028faf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b515d530a4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba177f9f13 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 54fb546133 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0056031ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9888cda97e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5f0745e34b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 29dcc37810 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a4e317d6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> be1167e028 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fc179cb2d5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e74e1d9b4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1d4e479464 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de00bd1943 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ea5f19f56b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33dcb1f603 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64c4b371aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8354d11583 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 46b82ddad2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8b0bfd55f9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8e7c6661dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d83ac69cc5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2adb521281 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 307fc2b238 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d705cac10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c296484bff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0fb40a8655 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2e820cbfbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a42015a87c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d6e3d58599 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ec072ceda6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72ff05570e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b0a7faa6b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94096ec6bd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d98e354b8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a52516cf49 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1351a5baa4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4002ece69 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7273b06d00 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cee8a0fb22 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 857489f8a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 083bb9617b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 32aba52002 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c630f9360 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b69b6b3ea1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 665046a461 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65c5b3f5d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbc20a86a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b1dea55b93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d3ca180ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 73d82b731c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c673847c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ebbfa035a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad7a284ebe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3fe983979 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d539392ecd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d9ec67b278 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4010468d90 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f98490578 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1417ba30dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6bef55a1ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ce7c794944 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19108609dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f1f448d9e0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b515d530a4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a0d7cc16ab (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2a68a987a1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ba177f9f13 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> df9cd4f22d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64c0971d8e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc6506f342 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0154226e18 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 54fb546133 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9f616db357 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8169a66b76 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e0056031ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 344200f1df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 70f268c58f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd363ff8dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d39861d7f1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 00681e3bee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 69016fbbb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4e989397f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9888cda97e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d73ae82492 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 86c0805e1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dba8cb28c4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d1d11691fd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95d2c10f10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5f0745e34b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eeb8ab478a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 805eebdbb2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d58a385c84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c9bb37cf42 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75810048c9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 29dcc37810 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afcf525d7d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92078ab70d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dbbcaa10d3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 00494e0bcf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 463cfc7dc8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f2b9fb3853 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c43c29e91c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad377df392 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e639fc30a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64c0e084b5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b44c2cac84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 69a4e317d6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> cdb062d5e9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 775feff6dd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 65460df3e6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3d40465720 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 38f2492609 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cb3028faf (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3adb85864f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3adb85864f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d774dc1d12 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d38e17d229 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc5f917a7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3adb85864f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 260d67442f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8e7c6661dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d83ac69cc5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7d705cac10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0fb40a8655 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 49d3846b23 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8e7c6661dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7d705cac10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8e7c6661dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7d705cac10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8e7c6661dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7d705cac10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 72ff05570e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 49d3846b23 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72ff05570e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd6dd934cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 49d3846b23 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4ee051914 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 237494b1cc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e5413df04f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 657054de4f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbc20a86a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b4002ece69 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b4002ece69 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbc20a86a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dbc20a86a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d6ea9dd115 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3b0a7faa6b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3f98490578 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3b0a7faa6b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0fb40a8655 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b0a7faa6b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d98e354b8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7273b06d00 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f98490578 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0fb40a8655 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d98e354b8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7273b06d00 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8169a66b76 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54fb546133 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 94096ec6bd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 857489f8a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 54fb546133 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9f616db357 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8169a66b76 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 857489f8a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d98e354b8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70f268c58f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e0056031ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> a52516cf49 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e0056031ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 344200f1df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 70f268c58f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 72ff05570e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1506607bc9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 72ff05570e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a43551b62 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc6506f342 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9f616db357 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8169a66b76 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 344200f1df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70f268c58f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b4002ece69 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7273b06d00 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 857489f8a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69016fbbb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c53246c9a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9b169d0847 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1351a5baa4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0cb3028faf (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d4e479464 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1417ba30dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d4e479464 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1417ba30dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e81892dbf2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4010468d90 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6b26d19d93 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0c12a2c53d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 074e4b10b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 657d996e28 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19108609dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 77f1dd8356 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
=======
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8b0bfd55f9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 307fc2b238 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 917dff0811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7520530797 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
=======
=======
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b515d530a4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ba177f9f13 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 54fb546133 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 344200f1df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e0056031ea (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1351a5baa4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6605802d86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a17ba1f69c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c6079ecae5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 665046a461 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 65c5b3f5d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5f0745e34b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 293d1588a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f5a7cbd0df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> be1167e028 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> be1167e028 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 54da398207 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8999b3f6b9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 32b12d8c0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9807b0926d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0f1a31c83 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1d4e479464 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a834a844c7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d4496a2f2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6bc7d58545 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> aa5808a811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 657d996e28 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33dcb1f603 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 64c4b371aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8354d11583 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 46b82ddad2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8b0bfd55f9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8e7c6661dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d83ac69cc5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7cf935d373 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2adb521281 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 307fc2b238 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7d705cac10 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ba2e891f3d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b2c3a7dbfe (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 20c82a075f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c296484bff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5d545f24ed (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0fb40a8655 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc833b1448 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2e820cbfbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a42015a87c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d6e3d58599 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ec072ceda6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72ff05570e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3b0a7faa6b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8509be1a9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 94096ec6bd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db696250b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d98e354b8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8781a5befd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a52516cf49 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1351a5baa4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b4002ece69 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7273b06d00 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 857489f8a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 083bb9617b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 083bb9617b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b69b6b3ea1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 665046a461 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 65c5b3f5d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 64fb12c7c1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 993f1161be (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 9b21f252a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1fcaabf843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fde50255e4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d793553561 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 275e53813 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9b5d82faee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> bc158f6396 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c81ba1bf5d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3fe983979 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b3fe983979 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7abafecb1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1468e59829 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9c750992ef (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 02a91455d8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> afc081190 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 65360f96a0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 97a911d79c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cbaecd4a0f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4010468d90 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 206fa4e8ca (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5b94153fb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3bba3c74cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3f98490578 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d5210ebd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1417ba30dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 19e79eeeaa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0121c0dbad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77f1dd8356 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 19108609dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 77f1dd8356 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bd1dd3a906 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 601ca649ae (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6449641e67 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b81683f4bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9da518fc54 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 39a2b37b5e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4bd5ce922d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7f28e8e2e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f1f448d9e0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d857d34521 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8524ef3a47 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e71095843 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dea536df78 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ee8e40a0aa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 30e5e390d1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b515d530a4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a0d7cc16ab (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 02fea6930b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cdbed26e09 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a773c6ef65 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 34fd077439 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2a68a987a1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b586a90765 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7196beb2e1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df9cd4f22d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2a68a987a1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64c0971d8e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ba177f9f13 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> df9cd4f22d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 64c0971d8e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 813cf035d0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2b9cd40059 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cc6506f342 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0154226e18 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> efae05a8fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6beb374626 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f2473f877f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f54ebeac7e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 36678e6f26 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1162bd8d94 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 79f83f5952 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 253f1c4780 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c475d1d395 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 54fb546133 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9f616db357 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8169a66b76 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ac82c3dce (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ed36e40e7c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 43c8deb91e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> aca68ab685 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b0d520fde (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c3557818f9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> fd0793971d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9bcc7591e5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26eceae791 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 344200f1df (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 70f268c58f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e41e9641d2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 98e607d2fa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2261abd45b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 511a34eee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 07196fc321 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25ba2a2a3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f6a71e35ea (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d46ff705f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5876429ba2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e0644f7991 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 69016fbbb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 95d2c10f10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 805eebdbb2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5f0745e34b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> eeb8ab478a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 805eebdbb2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d58a385c84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e6162be24a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a8b2b56cbd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d58a385c84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c9bb37cf42 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c9bb37cf42 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> df91a2fed3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3af9048aee (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> caac7355dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c1872b9c98 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 027426fcb2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f2492609 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c0bd8d3c52 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f2492609 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cb3028faf (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0cb3028faf (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 0da1bb22c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 908ffa0499 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7352715aad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e8dcc1e632 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6afbd16091 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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

=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
newJsonObject
~~~~~~~~~~~~~
=======
jsonArrayToObject
~~~~~~~~~~~~~~~~~

This function converts a JSON array into a JSON object, by using the index of each array element as the object
key. This is useful for GeoMesa's JSON attribute types, which currently require a top-level object and not an array.

=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
jsonPath
~~~~~~~~
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))

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

=======
>>>>>>> 3b5854c1bb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22c3a3a32c (GEOMESA-3109 Json array to object converter function (#2788))
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

=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
newJsonObject
~~~~~~~~~~~~~
=======
jsonArrayToObject
~~~~~~~~~~~~~~~~~

This function converts a JSON array into a JSON object, by using the index of each array element as the object
key. This is useful for GeoMesa's JSON attribute types, which currently require a top-level object and not an array.

=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
jsonPath
~~~~~~~~
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))

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

=======
>>>>>>> cc47bbe241 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b04d18f291 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42f971fb10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 81b15ae9e2 (GEOMESA-3109 Json array to object converter function (#2788))
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

=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
newJsonObject
~~~~~~~~~~~~~
=======
jsonArrayToObject
~~~~~~~~~~~~~~~~~

This function converts a JSON array into a JSON object, by using the index of each array element as the object
key. This is useful for GeoMesa's JSON attribute types, which currently require a top-level object and not an array.

=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b22bfaa87 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> a8e7273bb0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
jsonPath
~~~~~~~~
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))

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

=======
>>>>>>> 3b5854c1bb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22c3a3a32c (GEOMESA-3109 Json array to object converter function (#2788))
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

=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
newJsonObject
~~~~~~~~~~~~~
=======
jsonArrayToObject
~~~~~~~~~~~~~~~~~

This function converts a JSON array into a JSON object, by using the index of each array element as the object
key. This is useful for GeoMesa's JSON attribute types, which currently require a top-level object and not an array.

=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
jsonPath
~~~~~~~~
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))

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
