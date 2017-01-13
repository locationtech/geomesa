Implementation Details
======================

Limitations
-----------

Currently, the Cassandra DataStore only supports point/time data.  Fortunately, the vast majority of high volume
spatio-temporal datasets are 'event' data which map directly to the supported data type.  Additionally, the Cassandra
DataStore expects queries to have bbox or 'polygon within' and time-between predicates.  Additional predicates on any
attribute are supported, but they are applied during a post-processing phase.  See the TODO section for how to
optimize these predicates.

Index Structure
---------------

The Cassandra DataStore has a 32-bit integer as the primary key and a 64 bit integer as the clustering key, each with
the following structure.

**Partition/Primary Key (pkz)**

+---------------+-------------------------------+
| Bytes 31...16 | Byte 15...0                   |
+===============+===============================+
| Epoch Week    | 10-bit Z2 packed into 16 bits |
+---------------+-------------------------------+


**Clustering Key (z31)**

+---------------+
| Bytes 63...0  |
+===============+
| Full Z3       |
+---------------+

The week number since the epoch is encoded in the upper 16 bits of the primary key and a 10 bit Z2 index is encoded
in the lower 16 bits of the primary key.  This results in 1024 (10 bit Z2) primary partition keys per week.  For example,
a spatio-temporal point with lon/lat `-75.0,35.0` and dtg `2016-01-01T00:00:00.000Z` would have a primary key of
`157286595`. In addition to the primary key, the Cassandra DataStore encodes a Z3 index into the secondary sort index.  The Z3 index interleaves the latitude, longitude, and seconds in the current week into a 64 bit long.  See the TODO section for an
item regarding parameterizing the periodicity (Epoch Week).

Query Planning
--------------

In order to satisfy a spatio-temporal query, the Cassandra DataStore first computes all of the row-keys that intersect
the geospatial region as well as the temporal region.  Then, for each coarse geospatial region, the Cassandra DataStore
computes the Z3 intervals that cover the finer resolution spatio-temporal region.  It then issues a query for each
unique row and Z3 interval to get back the result sets.  Each result set is post-processed with any remaining
predicates on attributes.