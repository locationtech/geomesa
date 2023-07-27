"""
Build PySpark UDFs from the Geomesa UDFs (scala UDFs).
"""
from geomesa_pyspark.scala.udf import build_scala_udf, ColumnOrName
from pyspark import SparkContext
from pyspark.sql.column import Column

spark_context = SparkContext.getOrCreate()
geomesa_udfs = spark_context._jvm.org.locationtech.geomesa.spark.sql.GeomesaPysparkFunctions

# Geometric Accessor Functions

def st_boundary(col: ColumnOrName) -> Column:
    """Returns the boundary, or an empty geometry of appropriate dimension, if
    geom is empty."""
    return build_scala_udf(spark_context, geomesa_udfs.st_boundary)(col)

def st_coordDim(col: ColumnOrName) -> Column:
    """Returns the number of dimensions of the coordinates of Geometry geom."""
    return build_scala_udf(spark_context, geomesa_udfs.st_coordDim)(col)

def st_dimension(col: ColumnOrName) -> Column:
    """Returns the inherent number of dimensions of this Geometry object, which
    must be less than or equal to the coordinate dimension."""
    return build_scala_udf(spark_context, geomesa_udfs.st_dimension)(col)

def st_envelope(col: ColumnOrName) -> Column:
    """Returns a Geometry representing the bounding box of geom."""
    return build_scala_udf(spark_context, geomesa_udfs.st_envelope)(col)

def st_exteriorRing(col: ColumnOrName) -> Column:
    """Returns a LineString representing the exterior ring of the geometry;
    returns null if the Geometry is not a Polygon."""
    return build_scala_udf(spark_context, geomesa_udfs.st_exteriorRing)(col)

def st_geometryN(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the n-th Geometry (1-based index) of geom if the Geometry is a
    GeometryCollection, or geom if it is not."""
    return build_scala_udf(spark_context, geomesa_udfs.st_geometryN)(col1, col2)

def st_interiorRingN(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the n-th interior LineString ring of the Polygon geom. Returns
    null if the geometry is not a Polygon or the given n is out of range."""
    return build_scala_udf(spark_context, geomesa_udfs.st_interiorRingN)(col1, col2)

def st_isClosed(col: ColumnOrName) -> Column:
    """Returns true if geom is a LineString or MultiLineString and its start
    and end points are coincident. Returns true for all other Geometry types."""
    return build_scala_udf(spark_context, geomesa_udfs.st_isClosed)(col)

def st_isCollection(col: ColumnOrName) -> Column:
    """Returns true if geom is a GeometryCollection."""
    return build_scala_udf(spark_context, geomesa_udfs.st_isCollection)(col)

def st_isEmpty(col: ColumnOrName) -> Column:
    """Returns true if geom is empty."""
    return build_scala_udf(spark_context, geomesa_udfs.st_isEmpty)(col)

def st_isRing(col: ColumnOrName) -> Column:
    """Returns true if geom is a LineString or a MultiLineString and is both
    closed and simple."""
    return build_scala_udf(spark_context, geomesa_udfs.st_isRing)(col)

def st_isSimple(col: ColumnOrName) -> Column:
    """Returns true if geom has no anomalous geometric points, such as self
    intersection or self tangency."""
    return build_scala_udf(spark_context, geomesa_udfs.st_isSimple)(col)

def st_isValid(col: ColumnOrName) -> Column:
    """Returns true if the Geometry is topologically valid according to the
    OGC SFS specification."""
    return build_scala_udf(spark_context, geomesa_udfs.st_isValid)(col)

def st_numGeometries(col: ColumnOrName) -> Column:
    """If geom is a GeometryCollection, returns the number of geometries. For
    single geometries, returns 1."""
    return build_scala_udf(spark_context, geomesa_udfs.st_numGeometries)(col)

def st_numPoints(col: ColumnOrName) -> Column:
    """Returns the number of vertices in Geometry geom."""
    return build_scala_udf(spark_context, geomesa_udfs.st_numPoints)(col)

def st_pointN(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """If geom is a LineString, returns the n-th vertex of geom as a Point.
    Negative values are counted backwards from the end of the LineString.
    Returns null if geom is not a LineString."""
    return build_scala_udf(spark_context, geomesa_udfs.st_pointN)(col1, col2)

def st_x(col: ColumnOrName) -> Column:
    """If geom is a Point, return the X coordinate of that point."""
    return build_scala_udf(spark_context, geomesa_udfs.st_x)(col)

def st_y(col: ColumnOrName) -> Column:
    """If geom is a Point, return the Y coordinate of that point."""
    return build_scala_udf(spark_context, geomesa_udfs.st_y)(col)

# Geometric Cast Functions

def st_castToPoint(col: ColumnOrName) -> Column:
    """Casts Geometry g to a Point."""
    return build_scala_udf(spark_context, geomesa_udfs.st_castToPoint)(col)

def st_castToPolygon(col: ColumnOrName) -> Column:
    """Casts Geometry g to a Polygon."""
    return build_scala_udf(spark_context, geomesa_udfs.st_castToPolygon)(col)

def st_castToLineString(col: ColumnOrName) -> Column:
    """Casts Geometry g to a LineString."""
    return build_scala_udf(spark_context, geomesa_udfs.st_castToLineString)(col)

def st_castToGeometry(col: ColumnOrName) -> Column:
    """Casts Geometry subclass g to a Geometry. This can be necessary e.g. when
    storing the output of st_makePoint as a Geometry in a case class."""
    return build_scala_udf(spark_context, geomesa_udfs.st_castToGeometry)(col)

def st_byteArray(col: ColumnOrName) -> Column:
    """Encodes string s into an array of bytes using the UTF-8 charset."""
    return build_scala_udf(spark_context, geomesa_udfs.st_byteArray)(col)

# Geometric Constructor Functions

def st_geomFromGeoHash(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the Geometry of the bounding box corresponding to the Geohash
    string geohash (base-32 encoded) with a precision of prec bits. See Geohash
    for more information on GeoHashes."""
    return build_scala_udf(spark_context, geomesa_udfs.st_geomFromGeoHash)(col1, col2)

def st_box2DFromGeoHash(col: ColumnOrName) -> Column:
    """Alias of st_geomFromGeoHash."""
    return build_scala_udf(spark_context, geomesa_udfs.st_box2DFromGeoHash)(col)

def st_geomFromGeoJSON(col: ColumnOrName) -> Column:
    """"""
    return build_scala_udf(spark_context, geomesa_udfs.st_geomFromGeoJSON)(col)

def st_geomFromText(col: ColumnOrName) -> Column:
    """Alias of st_geomFromWKT."""
    return build_scala_udf(spark_context, geomesa_udfs.st_geomFromText)(col)

def st_geometryFromText(col: ColumnOrName) -> Column:
    """Alias of st_geomFromWKT"""
    return build_scala_udf(spark_context, geomesa_udfs.st_geometryFromText)(col)

def st_geomFromWKT(col: ColumnOrName) -> Column:
    """Creates a Geometry from the given Well-Known Text representation (WKT)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_geomFromWKT)(col)

def st_geomFromWKB(col: ColumnOrName) -> Column:
    """Creates a Geometry from the given Well-Known Binary representation (WKB)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_geomFromWKB)(col)

def st_lineFromText(col: ColumnOrName) -> Column:
    """Creates a LineString from the given WKT representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_lineFromText)(col)

def st_makeBox2D(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Creates a Geometry representing a bounding box defined by the given Points."""
    return build_scala_udf(spark_context, geomesa_udfs.st_makeBox2D)(col1, col2)

def st_makeBBOX(col1: ColumnOrName, col2: ColumnOrName, col3: ColumnOrName, col4: ColumnOrName) -> Column:
    """Creates a Geometry representing a bounding box with the given boundaries."""
    return build_scala_udf(spark_context, geomesa_udfs.st_makeBBOX)(col1, col2, col3, col4)

def st_makePolygon(col: ColumnOrName) -> Column:
    """Creates a Polygon formed by the given LineString shell, which must be closed."""
    return build_scala_udf(spark_context, geomesa_udfs.st_makePolygon)(col)

def st_makePoint(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Creates a Point with an x and y coordinate."""
    return build_scala_udf(spark_context, geomesa_udfs.st_makePoint)(col1, col2)

def st_makeLine(col: ColumnOrName) -> Column:
    """Creates a LineString using the given sequence of vertices in points."""
    return build_scala_udf(spark_context, geomesa_udfs.st_makeLine)(col)

def st_makePointM(col1: ColumnOrName, col2: ColumnOrName, col3: ColumnOrName) -> Column:
    """Creates a Point with an x, y, and m coordinate."""
    return build_scala_udf(spark_context, geomesa_udfs.st_makePointM)(col1, col2, col3)

def st_mLineFromText(col: ColumnOrName) -> Column:
    """Creates a MultiLineString corresponding to the given WKT representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_mLineFromText)(col)

def st_mPointFromText(col: ColumnOrName) -> Column:
    """Creates a MultiPoint corresponding to the given WKT representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_mPointFromText)(col)

def st_mPolyFromText(col: ColumnOrName) -> Column:
    """Creates a MultiPolygon corresponding to the given WKT representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_mPolyFromText)(col)

def st_point(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns a Point with the given coordinate values. This is an OGC alias
    for st_makePoint."""
    return build_scala_udf(spark_context, geomesa_udfs.st_point)(col1, col2)

def st_pointFromGeoHash(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Return the Point at the geometric center of the bounding box defined by
    the Geohash string geohash (base-32 encoded) with a precision of prec bits.
    See Geohash for more information on Geohashes."""
    return build_scala_udf(spark_context, geomesa_udfs.st_pointFromGeoHash)(col1, col2)

def st_pointFromText(col: ColumnOrName) -> Column:
    """Creates a Point corresponding to the given WKT representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_pointFromText)(col)

def st_pointFromWKB(col: ColumnOrName) -> Column:
    """Creates a Point corresponding to the given WKB representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_pointFromWKB)(col)

def st_polygon(col: ColumnOrName) -> Column:
    """Creates a Polygon formed by the given LineString shell, which must be
    closed."""
    return build_scala_udf(spark_context, geomesa_udfs.st_polygon)(col)

def st_polygonFromText(col: ColumnOrName) -> Column:
    """Creates a Polygon corresponding to the given WKT representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_polygonFromText)(col)

# Geometric Output Functions

def st_asBinary(col: ColumnOrName) -> Column:
    """Returns Geometry geom in WKB representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_asBinary)(col)

def st_asGeoJSON(col: ColumnOrName) -> Column:
    """Returns Geometry geom in GeoJSON representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_asGeoJSON)(col)

def st_asLatLonText(col: ColumnOrName) -> Column:
    """Returns a String describing the latitude and longitude of Point p in
    degrees, minutes, and seconds. (This presumes that the units of the
    coordinates of p are latitude and longitude.)"""
    return build_scala_udf(spark_context, geomesa_udfs.st_asLatLonText)(col)

def st_asText(col: ColumnOrName) -> Column:
    """Returns Geometry geom in WKT representation."""
    return build_scala_udf(spark_context, geomesa_udfs.st_asText)(col)

def st_geoHash(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the Geohash (in base-32 representation) of an interior point of
    Geometry geom. See Geohash for more information on Geohashes."""
    return build_scala_udf(spark_context, geomesa_udfs.st_geoHash)(col1, col2)

# Geometric Processing Functions

def st_antimeridianSafeGeom(col: ColumnOrName) -> Column:
    """If geom spans the antimeridian, attempt to convert the geometry into an
    equivalent form that is “antimeridian-safe” (i.e. the output geometry is
    covered by BOX(-180 -90, 180 90)). In certain circumstances, this method
    may fail, in which case the input geometry will be returned and an error
    will be logged."""
    return build_scala_udf(spark_context, geomesa_udfs.st_antimeridianSafeGeom)(col)

def st_bufferPoint(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns a Geometry covering all points within a given radius of Point p,
    where radius is given in meters."""
    return build_scala_udf(spark_context, geomesa_udfs.st_bufferPoint)(col1, col2)

def st_idlSafeGeom(col: ColumnOrName) -> Column:
    """Alias of st_antimeridianSafeGeom."""
    return build_scala_udf(spark_context, geomesa_udfs.st_antimeridianSafeGeom)(col)

<<<<<<< HEAD
def st_makeValid(col: ColumnOrName) -> Column:
    """Returns the repaired geometry."""
    return build_scala_udf(spark_context, geomesa_udfs.st_makeValid)(col)

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
# Spatial Relation Functions

def st_translate(col1: ColumnOrName, col2: ColumnOrName, col3: ColumnOrName) -> Column:
    """Returns the Geometry produced when geom is translated by deltaX and deltaY."""
    return build_scala_udf(spark_context, geomesa_udfs.st_translate)(col1, col2, col3)

def st_contains(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if and only if no points of b lie in the exterior of a, and
    at least one point of the interior of b lies in the interior of a."""
    return build_scala_udf(spark_context, geomesa_udfs.st_contains)(col1, col2)

def st_covers(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if no point in Geometry b is outside Geometry a."""
    return build_scala_udf(spark_context, geomesa_udfs.st_covers)(col1, col2)

def st_crosses(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if the supplied geometries have some, but not all, interior
    points in common."""
    return build_scala_udf(spark_context, geomesa_udfs.st_crosses)(col1, col2)

def st_disjoint(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if the geometries do not “spatially intersect”; i.e., they
    do not share any space together. Equivalent to NOT st_intersects(a, b)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_disjoint)(col1, col2)

def st_equals(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if the given Geometries represent the same logical
    Geometry. Directionality is ignored."""
    return build_scala_udf(spark_context, geomesa_udfs.st_equals)(col1, col2)

def st_intersects(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if the geometries spatially intersect in 2D (i.e. share any
    portion of space). Equivalent to NOT st_disjoint(a, b)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_intersects)(col1, col2)

def st_overlaps(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if the geometries have some but not all points in common,
    are of the same dimension, and the intersection of the interiors of the two
    geometries has the same dimension as the geometries themselves."""
    return build_scala_udf(spark_context, geomesa_udfs.st_overlaps)(col1, col2)

def st_touches(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if the geometries have at least one point in common, but
    their interiors do not intersect."""
    return build_scala_udf(spark_context, geomesa_udfs.st_touches)(col1, col2)

def st_within(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns true if geometry a is completely inside geometry b."""
    return build_scala_udf(spark_context, geomesa_udfs.st_within)(col1, col2)

def st_relate(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the DE-9IM 3x3 interaction matrix pattern describing the
    dimensionality of the intersections between the interior, boundary and
    exterior of the two geometries."""
    return build_scala_udf(spark_context, geomesa_udfs.st_relate)(col1, col2)

def st_relateBool(col1: ColumnOrName, col2: ColumnOrName, col3: ColumnOrName) -> Column:
    """Returns true if the DE-9IM interaction matrix mask mask matches the
    interaction matrix pattern obtained from st_relate(a, b)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_relateBool)(col1, col2, col3)

def st_area(col: ColumnOrName) -> Column:
    """If Geometry g is areal, returns the area of its surface in square units
    of the coordinate reference system (for example, degrees^2 for EPSG:4326).
    Returns 0.0 for non-areal geometries (e.g. Points, non-closed LineStrings,
    etc.)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_area)(col)

def st_centroid(col: ColumnOrName) -> Column:
    """Returns the geometric center of a geometry."""
    return build_scala_udf(spark_context, geomesa_udfs.st_centroid)(col)

def st_closestPoint(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the Point on a that is closest to b. This is the first point of
    the shortest line."""
    return build_scala_udf(spark_context, geomesa_udfs.st_closestPoint)(col1, col2)

def st_distance(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the 2D Cartesian distance between the two geometries in units of
    the coordinate reference system (e.g. degrees for EPSG:4236)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_distance)(col1, col2)

def st_distanceSphere(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Approximates the minimum distance between two longitude/latitude
    geometries assuming a spherical earth."""
    return build_scala_udf(spark_context, geomesa_udfs.st_distanceSphere)(col1, col2)

def st_distanceSpheroid(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the minimum distance between two longitude/latitude geometries
    assuming the WGS84 spheroid."""
    return build_scala_udf(spark_context, geomesa_udfs.st_distanceSpheroid)(col1, col2)

def st_length(col: ColumnOrName) -> Column:
    """Returns the 2D path length of linear geometries, or perimeter of areal
    geometries, in units of the the coordinate reference system (e.g. degrees
    for EPSG:4236). Returns 0.0 for other geometry types (e.g. Point)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_length)(col)

def st_lengthSphere(col: ColumnOrName) -> Column:
    """Approximates the 2D path length of a LineString geometry using a
    spherical earth model. The returned length is in units of meters. The
    approximation is within 0.3% of st_lengthSpheroid and is computationally
    more efficient."""
    return build_scala_udf(spark_context, geomesa_udfs.st_lengthSphere)(col)

def st_lengthSpheroid(col: ColumnOrName) -> Column:
    """Calculates the 2D path length of a LineString geometry defined with
    longitude/latitude coordinates on the WGS84 spheroid. The returned length
    is in units of meters."""
    return build_scala_udf(spark_context, geomesa_udfs.st_lengthSpheroid)(col)

def st_intersection(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the intersection of the input geometries."""
    return build_scala_udf(spark_context, geomesa_udfs.st_intersection)(col1, col2)

def st_difference(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Returns the difference of the input geometries."""
    return build_scala_udf(spark_context, geomesa_udfs.st_difference)(col1, col2)

def st_transform(col1: ColumnOrName, col2: ColumnOrName, col3: ColumnOrName) -> Column:
    """Returns a new geometry with its coordinates transformed to a different
    coordinate reference system (for example from EPSG:4326 to EPSG:27700)."""
    return build_scala_udf(spark_context, geomesa_udfs.st_transform)(col1, col2, col3)