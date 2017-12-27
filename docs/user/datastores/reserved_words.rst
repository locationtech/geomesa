.. _reserved-words:

Reserved Words
--------------

There are a number of GeoTools reserved words that cannot be used as attribute names in GeoMesa.
Note that this list is case insensitive.

* AFTER
* AND
* BEFORE
* BEYOND
* CONTAINS
* CROSSES
* DISJOINT
* DOES-NOT-EXIST
* DURING
* DWITHIN
* EQUALS
* EXCLUDE
* EXISTS
* FALSE
* GEOMETRYCOLLECTION
* ID
* INCLUDE
* INTERSECTS
* IS
* LIKE
* LINESTRING
* LOCATION
* MULTILINESTRING
* MULTIPOINT
* MULTIPOLYGON
* NOT
* NULL
* OR
* OVERLAPS
* POINT
* POLYGON
* RELATE
* TOUCHES
* TRUE
* WITHIN

Override
^^^^^^^^

If you really, really want to use one of these words as an attribute name, you may override the check. Note that some
functionality may not work, however.

Override the check by setting the ``SimpleFeatureType`` user data key ``override.reserved.words=true``. Alternatively,
you may set it as a system property.
