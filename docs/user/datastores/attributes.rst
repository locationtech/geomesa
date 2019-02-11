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

* Only a single geometry-type attribute may be indexed. It will be included in the primary spatial
  and spatio-temporal indices.
* The primary date-type attribute will be used in the spatio-temporal index. It may also be indexed
  separately in an attribute index.
* Non-geometry types (including dates) may be indexed as separate attribute indices. For details, see
  :ref:`attribute_indices`.
* Container types (List and Map) must be parameterized with non-container types from the above table.

.. warning::

  List types may be indexed, but querying a list type may result in duplicate results. If duplicate
  results are a problem, users should implement their own de-duplication logic for list queries
