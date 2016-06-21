GeoMesa Data Stores
===================

GeoMesa provides several GeoTools-compatible data stores for several distributed
column-oriented databases, as well as the Kafka messaging system. These data
stores are described in the following chapters. More information on using GeoTools
can be found in the `GeoTools user guide <http://docs.geotools.org/stable/userguide/>`__.

The sections below describe features and caveats common to all GeoMesa GeoTools data
stores.

Reserved Words
--------------

There are a number of reserved words that cannot be used as attribute names in GeoMesa.
Note that this list is case insensitive!

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