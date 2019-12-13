SparkSQL Functions
==================

The following is a list of the spatial SparkSQL user-defined functions defined by the ``geomesa-spark-sql`` module.

.. contents::
    :local:

Geometry Constructors
---------------------

.. _st_box2DFromGeoHash:

st_box2DFromGeoHash
^^^^^^^^^^^^^^^^^^^

::

    Geometry st_box2DFromGeoHash(String geohash, Int prec)

Alias of :ref:`st_geomFromGeoHash`.

.. _st_geomFromGeoHash:

st_geomFromGeoHash
^^^^^^^^^^^^^^^^^^

::

    Geometry st_geomFromGeoHash(String geohash, Int prec)

Returns the ``Geometry`` of the bounding box corresponding to the Geohash string *geohash* (base-32 encoded) with
a precision of *prec* bits. See :ref:`geohash` for more information on GeoHashes.

.. _st_geomFromText:

st_geomFromText
^^^^^^^^^^^^^^^

::

    Geometry st_geomFromText(String wkt)

Alias of :ref:`st_geomFromWKT`.

.. _st_geomFromWKB:

st_geomFromWKB
^^^^^^^^^^^^^^

::

    Geometry st_geomFromWKB(Array[Byte] wkb)

Creates a ``Geometry`` from the given Well-Known Binary representation (`WKB`_).

.. _st_geomFromWKT:

st_geomFromWKT
^^^^^^^^^^^^^^

::

    Geometry st_geomFromWKT(String wkt)

Creates a Geometry from the given Well-Known Text representation (`WKT`_).

.. _st_geometryFromText:

st_geometryFromText
^^^^^^^^^^^^^^^^^^^

::

    Geometry st_geometryFromText(String wkt)

Alias of :ref:`st_geomFromWKT`

.. _st_lineFromText:

st_lineFromText
^^^^^^^^^^^^^^^

::

    LineString st_lineFromText(String wkt)

Creates a ``LineString`` from the given `WKT`_ representation.

.. _st_mLineFromText:

st_mLineFromText
^^^^^^^^^^^^^^^^

::

    MultiLineString st_mLineFromText(String wkt)

Creates a ``MultiLineString`` corresponding to the given `WKT`_ representation.

.. _st_mPointFromText:

st_mPointFromText
^^^^^^^^^^^^^^^^^

::

    MultiPoint st_mPointFromText(String wkt)

Creates a ``MultiPoint`` corresponding to the given `WKT`_ representation.

.. _st_mPolyFromText:

st_mPolyFromText
^^^^^^^^^^^^^^^^

::

    MultiPolygon st_mPolyFromText(String wkt)

Creates a ``MultiPolygon`` corresponding to the given `WKT`_ representation.

.. _st_makeBBOX:

st_makeBBOX
^^^^^^^^^^^

::

    Geometry st_makeBBOX(Double lowerX, Double lowerY, Double upperX, Double upperY)

Creates a ``Geometry`` representing a bounding box with the given boundaries.


.. _st_makeBox2D:

st_makeBox2D
^^^^^^^^^^^^

::

    Geometry st_makeBox2D(Point lowerLeft, Point upperRight)

Creates a ``Geometry`` representing a bounding box defined by the given ``Point``\ s.

.. _st_makeLine:

st_makeLine
^^^^^^^^^^^

::

    LineString st_makeLine(Seq[Point] points)

Creates a ``LineString`` using the given sequence of vertices in *points*.

.. _st_makePoint:

st_makePoint
^^^^^^^^^^^^

::

    Point st_makePoint(Double x, Double y)

Creates a ``Point`` with an *x* and *y* coordinate.

.. _st_makePointM:

st_makePointM
^^^^^^^^^^^^^

::

    Point st_makePointM(Double x, Double y, Double m)

Creates a ``Point`` with an *x*, *y*, and *m* coordinate.

.. _st_makePolygon:

st_makePolygon
^^^^^^^^^^^^^^

::

    Polygon st_makePolygon(LineString shell)

Creates a ``Polygon`` formed by the given ``LineString`` *shell*, which must be closed.

.. _st_point:

st_point
^^^^^^^^

::

    Point st_point(Double x, Double y)

Returns a ``Point`` with the given coordinate values. This is an OGC alias for :ref:`st_makePoint`.

.. _st_pointFromGeoHash:

st_pointFromGeoHash
^^^^^^^^^^^^^^^^^^^

::

    Point st_pointFromGeoHash(String geohash, Int prec)

Return the ``Point`` at the geometric center of the bounding box defined by the Geohash string *geohash*
(base-32 encoded) with a precision of *prec* bits. See :ref:`geohash` for more information on Geohashes.

.. _st_pointFromText:

st_pointFromText
^^^^^^^^^^^^^^^^

::

    Point st_pointFromText(String wkt)

Creates a ``Point`` corresponding to the given `WKT`_ representation.

.. _st_pointFromWKB:

st_pointFromWKB
^^^^^^^^^^^^^^^

::

    Point st_pointFromWKB(Array[Byte] wkb)

Creates a ``Point`` corresponding to the given `WKB`_ representation.

.. _st_polygon:

st_polygon
^^^^^^^^^^

::

    Polygon st_polygon(LineString shell)

Creates a ``Polygon`` formed by the given ``LineString`` *shell*, which must be closed.

.. _st_polygonFromText:

st_polygonFromText
^^^^^^^^^^^^^^^^^^

::

    Polygon st_polygonFromText(String wkt)

Creates a ``Polygon`` corresponding to the given `WKT`_ representation.

Geometry Accessors
------------------

.. _st_boundary:

st_boundary
^^^^^^^^^^^

::

    Geometry st_boundary(Geometry geom)

Returns the boundary, or an empty geometry of appropriate dimension, if *geom* is empty.

.. _st_coordDim:

st_coordDim
^^^^^^^^^^^

::

    Int st_coordDim(Geometry geom)

Returns the number of dimensions of the coordinates of ``Geometry`` *geom*.

.. _st_dimension:

st_dimension
^^^^^^^^^^^^

::

    Int st_dimension(Geometry geom)

Returns the inherent number of dimensions of this ``Geometry`` object, which must be less than or equal to the
coordinate dimension.

.. _st_envelope:

st_envelope
^^^^^^^^^^^

::

    Geometry st_envelope(Geometry geom)

Returns a ``Geometry`` representing the bounding box of *geom*.

.. _st_exteriorRing:

st_exteriorRing
^^^^^^^^^^^^^^^

::

    LineString st_exteriorRing(Geometry geom)

Returns a ``LineString`` representing the exterior ring of the geometry; returns null if the ``Geometry`` is not
a ``Polygon``.

.. _st_geometryN:

st_geometryN
^^^^^^^^^^^^

::

    Int st_geometryN(Geometry geom, Int n)

Returns the *n*-th ``Geometry`` (1-based index) of *geom* if the ``Geometry`` is a ``GeometryCollection``, or
*geom* if it is not.

.. _st_interiorRingN:

st_interiorRingN
^^^^^^^^^^^^^^^^

::

    Int st_interiorRingN(Geometry geom, Int n)

Returns the *n*-th interior ``LineString`` ring of the ``Polygon`` *geom*. Returns null if the geometry is not
a ``Polygon`` or the given *n* is out of range.

.. _st_isClosed:

st_isClosed
^^^^^^^^^^^

::

    Boolean st_isClosed(Geometry geom)

Returns true if *geom* is a ``LineString`` or ``MultiLineString`` and its start and end points are coincident.
Returns true for all other ``Geometry`` types.

.. _st_isCollection:

st_isCollection
^^^^^^^^^^^^^^^

::

    Boolean st_isCollection(Geometry geom)

Returns true if *geom* is a ``GeometryCollection``.

.. _st_isEmpty:

st_isEmpty
^^^^^^^^^^

::

    Boolean st_isEmpty(Geometry geom)

Returns true if *geom* is empty.

.. _st_isRing:

st_isRing
^^^^^^^^^

::

    Boolean st_isRing(Geometry geom)

Returns true if *geom* is a ``LineString`` or a ``MultiLineString`` and is both closed and simple.

.. _st_isSimple:

st_isSimple
^^^^^^^^^^^

::

    Boolean st_isSimple(Geometry geom)

Returns true if *geom* has no anomalous geometric points, such as self intersection or self tangency.

.. _st_isValid:

st_isValid
^^^^^^^^^^

::

    Boolean st_isValid(Geometry geom)

Returns true if the ``Geometry`` is topologically valid according to the OGC SFS specification.

.. _st_numGeometries:

st_numGeometries
^^^^^^^^^^^^^^^^

::

    Int st_numGeometries(Geometry geom)

If *geom* is a ``GeometryCollection``, returns the number of geometries. For single geometries, returns 1,

.. _st_numPoints:

st_numPoints
^^^^^^^^^^^^

::

    Int st_numPoints(Geometry geom)

Returns the number of vertices in ``Geometry`` *geom*.

.. _st_pointN:

st_pointN
^^^^^^^^^

::

    Point st_pointN(Geometry geom, Int n)

If *geom* is a ``LineString``, returns the *n*-th vertex of *geom* as a Point. Negative values are counted
backwards from the end of the ``LineString``. Returns null if *geom* is not a ``LineString``.

.. _st_x:

st_x
^^^^

::

    Float st_X(Geometry geom)

If *geom* is a ``Point``, return the X coordinate of that point.

.. _st_y:

st_y
^^^^

::

    Float st_y(Geometry geom)

If *geom* is a ``Point``, return the Y coordinate of that point.

Geometry Cast
-------------

.. _st_castToLineString:

st_castToLineString
^^^^^^^^^^^^^^^^^^^

::

    LineString st_castToLineString(Geometry g)

Casts ``Geometry`` *g* to a ``LineString``.

.. _st_castToPoint:

st_castToPoint
^^^^^^^^^^^^^^

::

    Point st_castToPoint(Geometry g)

Casts ``Geometry`` *g* to a ``Point``.

.. _st_castToPolygon:

st_castToPolygon
^^^^^^^^^^^^^^^^

::

    Polygon st_castToPolygon(Geometry g)

Casts ``Geometry`` *g* to a ``Polygon``.

.. _st_castToGeometry:

st_castToGeometry
^^^^^^^^^^^^^^^^^

::

    Geometry st_castToGeometry(Geometry g)

Casts ``Geometry`` subclass *g* to a ``Geometry``. This can be necessary e.g. when storing the output of
``st_makePoint`` as a ``Geometry`` in a case class.

.. _st_byteArray:

st_byteArray
^^^^^^^^^^^^

::

    Array[Byte] st_byteArray(String s)

Encodes string *s* into an array of bytes using the UTF-8 charset.

Geometry Editors
----------------

.. _st_translate:

st_translate
^^^^^^^^^^^^

::

    Geometry st_translate(Geometry geom, Double deltaX, Double deltaY)

Returns the ``Geometry`` produced when *geom* is translated by *deltaX* and *deltaY*.


Geometry Outputs
----------------

.. _st_asBinary:

st_asBinary
^^^^^^^^^^^

::

     Array[Byte] st_asBinary(Geometry geom)

Returns ``Geometry`` *geom* in `WKB`_ representation.

.. _st_asGeoJSON:

st_asGeoJSON
^^^^^^^^^^^^

::

     String st_asGeoJSON(Geometry geom)

Returns ``Geometry`` *geom* in `GeoJSON`_ representation.

.. _GeoJSON: http://geojson.org/

.. _st_asLatLonText:

st_asLatLonText
^^^^^^^^^^^^^^^

::

     String st_asLatLonText(Point p)

Returns a ``String`` describing the latitude and longitude of ``Point`` *p* in degrees, minutes, and seconds.
(This presumes that the units of the coordinates of *p* are latitude and longitude.)

.. _st_asText:

st_asText
^^^^^^^^^

::

    String st_asText(Geometry geom)

Returns ``Geometry`` *geom* in `WKT`_ representation.

.. _st_geoHash:

st_geoHash
^^^^^^^^^^

::

    String st_geoHash(Geometry geom, Int prec)

Returns the Geohash (in base-32 representation) of an interior point of Geometry *geom*. See :ref:`geohash` for
more information on Geohashes.


Spatial Relationships
---------------------

.. _st_area:

st_area
^^^^^^^

::

    Double st_area(Geometry g)

If ``Geometry`` *g* is areal, returns the area of its surface in square units of the coordinate reference system
(for example, degrees^2 for EPSG:4326). Returns 0.0 for non-areal geometries (e.g. ``Point``\ s, non-closed
``LineString``\ s, etc.).

.. _st_centroid:

st_centroid
^^^^^^^^^^^

::

    Point st_centroid(Geometry g)

Returns the geometric center of a geometry.

.. _st_closestPoint:

st_closestPoint
^^^^^^^^^^^^^^^

::

    Point st_closestPoint(Geometry a, Geometry b)

Returns the ``Point`` on *a* that is closest to *b*. This is the first point of the shortest line.

.. _st_contains:

st_contains
^^^^^^^^^^^

::

    Boolean st_contains(Geometry a, Geometry b)

Returns true if and only if no points of *b* lie in the exterior of *a*, and at least one point of the interior
of *b* lies in the interior of *a*.

.. _st_covers:

st_covers
^^^^^^^^^

::

    Boolean st_covers(Geometry a, Geometry b)

Returns true if no point in ``Geometry`` *b* is outside ``Geometry`` *a*.

.. _st_crosses:

st_crosses
^^^^^^^^^^

::

    Boolean st_crosses(Geometry a, Geometry b)

Returns true if the supplied geometries have some, but not all, interior points in common.

.. _st_disjoint:

st_disjoint
^^^^^^^^^^^

::

    Boolean st_disjoint(Geometry a, Geometry b)

Returns true if the geometries do not "spatially intersect"; i.e., they do not share any space together. Equivalent
to ``NOT st_intersects(a, b)``.

.. _st_distance:

st_distance
^^^^^^^^^^^

::

    Double st_distance(Geometry a, Geometry b)

Returns the 2D Cartesian distance between the two geometries in units of the coordinate reference system (e.g.
degrees for EPSG:4236).

.. _st_distanceSphere:

st_distanceSphere
^^^^^^^^^^^^^^^^^

::

    Double st_distanceSphere(Geometry a, Geometry b)

Approximates the minimum distance between two longitude/latitude geometries assuming a spherical earth.

.. _st_distanceSpheroid:

st_distanceSpheroid
^^^^^^^^^^^^^^^^^^^

::

    Double st_distanceSpheroid(Geometry a, Geometry b)

Returns the minimum distance between two longitude/latitude geometries assuming the WGS84 spheroid.

.. _st_equals:

st_equals
^^^^^^^^^

::

    Boolean st_equals(Geometry a, Geometry b)

Returns true if the given Geometries represent the same logical Geometry. Directionality is ignored.

.. _st_intersects:

st_intersects
^^^^^^^^^^^^^

::

    Boolean st_intersects(Geometry a, Geometry b)

Returns true if the geometries spatially intersect in 2D (i.e. share any portion of space). Equivalent to
``NOT st_disjoint(a, b)``.

.. _st_length:

st_length
^^^^^^^^^

::

    Double st_length(Geometry geom)

Returns the 2D path length of linear geometries, or perimeter of areal geometries, in units of the the coordinate
reference system (e.g. degrees for EPSG:4236). Returns 0.0 for other geometry types (e.g. Point).

.. _st_lengthSphere:

st_lengthSphere
^^^^^^^^^^^^^^^

::

    Double st_lengthSphere(LineString line)

Approximates the 2D path length of a ``LineString`` geometry using a spherical earth model. The returned length is
in units of meters. The approximation is within 0.3% of st_lengthSpheroid and is computationally more efficient.

.. _st_lengthSpheroid:

st_lengthSpheroid
^^^^^^^^^^^^^^^^^

::

    Double st_lengthSpheroid(LineString line)

Calculates the 2D path length of a ``LineString`` geometry defined with longitude/latitude coordinates on the WGS84
spheroid. The returned length is in units of meters.

.. _st_overlaps:

st_overlaps
^^^^^^^^^^^

::

    Boolean st_overlaps(Geometry a, Geometry b)

Returns true if the geometries have some but not all points in common, are of the same dimension, and the intersection
of the interiors of the two geometries has the same dimension as the geometries themselves.

.. _st_relate:

st_relate
^^^^^^^^^

::

    String st_relate(Geometry a, Geometry b)

Returns the `DE-9IM`_ 3x3 interaction matrix pattern describing the dimensionality of the intersections between the
interior, boundary and exterior of the two geometries.

.. _st_relateBool:

st_relateBool
^^^^^^^^^^^^^

::

    Boolean st_relateBool(Geometry a, Geometry b, String mask)

Returns true if the `DE-9IM`_ interaction matrix mask *mask* matches the interaction matrix pattern obtained from
``st_relate(a, b)``.


.. _st_touches:

st_touches
^^^^^^^^^^

::

    Boolean st_touches(Geometry a, Geometry b)

Returns true if the geometries have at least one point in common, but their interiors do not intersect.

.. _st_within:

st_within
^^^^^^^^^

::

    Boolean st_within(Geometry a, Geometry b)

Returns true if geometry *a* is completely inside geometry *b*.

Geometry Processing
-------------------

.. _st_antimeridianSafeGeom:

st_antimeridianSafeGeom
^^^^^^^^^^^^^^^^^^^^^^^

::

    Geometry st_antimeridianSafeGeom(Geometry geom)

If *geom* spans the `antimeridian`_, attempt to convert the geometry into an equivalent form that is
"antimeridian-safe" (i.e. the output geometry is covered by ``BOX(-180 -90, 180 90)``). In certain circumstances,
this method may fail, in which case the input geometry will be returned and an error will be logged.

.. _st_bufferPoint:

st_bufferPoint
^^^^^^^^^^^^^^

::

    Geometry st_bufferPoint(Point p, Double buffer)

Returns a ``Geometry`` covering all points within a given *radius* of ``Point`` *p*, where *radius* is given in meters.

.. _st_convexHull:

st_convexHull
^^^^^^^^^^^^^

::

    Geometry st_convexHull(Geometry geom)

**Aggregate function.** The convex hull of a geometry represents the minimum convex geometry that encloses all
geometries *geom* in the aggregated rows.

.. _st_idlSafeGeom:

st_idlSafeGeom
^^^^^^^^^^^^^^

Alias of :ref:`st_antimeridianSafeGeom`.

.. _antimeridian: https://en.wikipedia.org/wiki/180th_meridian

.. _DE-9IM: https://en.wikipedia.org/wiki/DE-9IM

.. _WKB: https://en.wikipedia.org/wiki/Well-known_text

.. _WKT: https://en.wikipedia.org/wiki/Well-known_text
