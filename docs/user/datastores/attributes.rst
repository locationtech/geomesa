.. _attribute_types:

GeoTools Feature Types
======================

A ``SimpleFeatureType`` defines a GeoTools schema, and consists of an array of well-known attributes. GeoMesa
supports all of the standard GeoTools attribute types, as well as some additional ones. When creating
a ``SimpleFeatureType`` for use in GeoMesa, be sure to use the provided classes, instead of the standard
GeoTools ``DataUtilities``:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

        SimpleFeatureTypes.createType("example", "name:String,dtg:Date,*geom:Point:srid=4326");

    .. code-tab:: scala

        import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

        SimpleFeatureTypes.createType("example", "name:String,dtg:Date,*geom:Point:srid=4326")

Available Types
---------------

================== ============================================== =========
Attribute Type     Binding                                        Indexable
================== ============================================== =========
String             java.lang.String                               Yes
Integer            java.lang.Integer                              Yes
Double             java.lang.Double                               Yes
Long               java.lang.Long                                 Yes
Float              java.lang.Float                                Yes
Boolean            java.lang.Boolean                              Yes
UUID               java.util.UUID                                 Yes
Date               java.util.Date                                 Yes
Timestamp          java.sql.Timestamp                             Yes
Point              org.locationtech.jts.geom.Point                Yes
LineString         org.locationtech.jts.geom.LineString           Yes
Polygon            org.locationtech.jts.geom.Polygon              Yes
MultiPoint         org.locationtech.jts.geom.MultiPoint           Yes
MultiLineString    org.locationtech.jts.geom.MultiLineString      Yes
MultiPolygon       org.locationtech.jts.geom.MultiPolygon         Yes
GeometryCollection org.locationtech.jts.geom.GeometryCollection   Yes
Geometry           org.locationtech.jts.geom.Geometry             Yes
List[A]            java.util.List<A>                              Yes
Map[A,B]           java.util.Map<A, B>                            No
Bytes              byte[]                                         No
================== ============================================== =========

Notes
^^^^^
* For details on indexing, see :ref:`creating_indices`.
* Container types (List and Map) must be parameterized with non-container types from the above table.

.. _set_sft_options:

Setting Schema Options
----------------------

Most configuration options for a feature type are specified through "user data", either on the ``SimpleFeatureType`` or on a
particular attribute. Setting the user data can be done in multiple ways.

If you are using a string to indicate your ``SimpleFeatureType``, you can append the type-level options to the end of
the string, like so:

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    // append the user-data values to the end of the string, separated by a semi-colon
    String spec = "name:String,dtg:Date,*geom:Point:srid=4326;option.one='foo',option.two='bar'";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``,
you may set the values directly in the feature type:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getUserData().put("option.one", "foo");

If you are using TypeSafe configuration files to define your simple feature type, you may include a ``user-data`` key:

.. code-block:: javascript

    geomesa.sfts.mySft = {
      attributes = [
        { name = name, type = String             }
        { name = dtg,  type = Date               }
        { name = geom, type = Point, srid = 4326 }
      ]
      user-data = {
        option.one = "foo"
      }
    }

.. _attribute_options:

Setting Attribute Options
-------------------------

In addition to schema-level user data, each attribute also has user data associated with it. Just like
the schema options, attribute user data can be set in multiple ways.

If you are using a string to indicate your ``SimpleFeatureType``, you can append the attribute options after the attribute type,
separated with a colon:

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    // append the user-data after the attribute type, separated by a colon
    String spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4326";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``, you may set the user
data directly in the attribute descriptor:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getDescriptor("name").getUserData().put("index", "true");

If you are using TypeSafe configuration files to define your simple feature type, you may add user data keys to the attribute
elements:

.. code-block:: javascript

    geomesa.sfts.mySft = {
      attributes = [
        { name = name, type = String, index = true }
        { name = dtg,  type = Date                 }
        { name = geom, type = Point, srid = 4326   }
      ]
    }
