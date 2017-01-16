GeoMesa Index Structure
=======================

GeoMesa uses a number of different indices in order to satisfy various search predicates:

- **Z2** - the Z2 index uses a two-dimensional Z-order curve to index latitude and longitude
  for point data. This index will be created if the feature type has the geometry type
  ``Point``. This is used to efficiently answer queries with a spatial component but no
  temporal component.
- **Z3** - the Z3 index uses a three-dimensional Z-order curve to index latitude, longitude,
  and time for point data. This index will be created if the feature type has the geometry
  type ``Point`` and has a time attribute. This is used to efficiently answer queries with
  both spatial and temporal components.
- **XZ2** - the XZ2 index uses a two-dimensional implementation of XZ-ordering [#ref1]_ to index
  latitude and longitude for non-point data. XZ-ordering is an extension of Z-ordering
  designed for spatially extended objects (i.e. non-point geometries such as line strings or
  polygons). This index will be created if the feature type has a non-\ ``Point`` geometry. This
  is used to efficiently answer queries with a spatial component but no temporal component.
- **XZ3** - the XZ3 index uses a three-dimensional implementation of XZ-ordering [#ref1]_ to index
  latitude, longitude, and time for non-point data. This index will be created if the feature
  type has a non-\ ``Point`` geometry and has a time attribute. This is used to efficiently
  answer queries with both spatial and temporal components.
- **Record/ID** - the record index uses feature ID as the primary key. It is used for any query by ID.
  Additionally, certain attribute queries may end up retrieving data from the record index.
  This is explained below.

If specified, GeoMesa will also create the following indices:

- **Attribute** - the attribute index uses attribute values as the primary index key. This allows for
  fast retrieval of queries without a spatio-temporal component. Attribute indices can be created
  in a 'reduced' format, that takes up less disk space at the cost of performance for certain queries,
  or a 'full' format that takes up more space but can answer certain queries faster.

.. rubric:: Footnotes

.. [#ref1] Böhm, Klump, and Kriegel. "XZ-ordering: a space-filling curve for objects with spatial extension." 6th. Int. Symposium on Large Spatial Databases (SSD), 1999, Hong Kong, China. (http://www.dbs.ifi.lmu.de/Publikationen/Boehm/Ordering_99.pdf)
