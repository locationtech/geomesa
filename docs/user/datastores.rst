GeoMesa Data Stores
===================

GeoMesa provides several GeoTools-compatible data stores for several distributed
column-oriented databases. These data
stores are described in the following chapters:

 * :doc:`/user/accumulo/index`
 * :doc:`/user/hbase/index`
 * :doc:`/user/bigtable/index`
 * :doc:`/user/cassandra/index`

There is also a GeoTools-compatible data store for the Kafka messaging system:

 * :doc:`/usr/kafka/index`


More information on using GeoTools
can be found in the `GeoTools user guide <http://docs.geotools.org/stable/userguide/>`__.

Common Features of GeoMesa Data Stores
--------------------------------------

The sections below describe features and caveats common to all GeoMesa data
stores.

.. _reserved-words:

Reserved Words
^^^^^^^^^^^^^^

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