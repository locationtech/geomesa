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
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
<<<<<<< HEAD
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
=======
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
=======
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
