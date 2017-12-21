.. _index_overview:

Index Overview
==============

GeoMesa uses a number of different indices in order to satisfy various search predicates. Each index
has an identifier (in brackets) which is used to reference it in configuration options.

- **Z2** [``z2``] - the Z2 index uses a two-dimensional Z-order curve to index latitude and longitude
  for point data. This index will be created if the feature type has the geometry type
  ``Point``. This is used to efficiently answer queries with a spatial component but no
  temporal component.
- **Z3** [``z3``] - the Z3 index uses a three-dimensional Z-order curve to index latitude, longitude,
  and time for point data. This index will be created if the feature type has the geometry
  type ``Point`` and has a time attribute. This is used to efficiently answer queries with
  both spatial and temporal components.
- **XZ2** [``xz2``] - the XZ2 index uses a two-dimensional implementation of XZ-ordering [#ref1]_ to index
  latitude and longitude for non-point data. XZ-ordering is an extension of Z-ordering
  designed for spatially extended objects (i.e. non-point geometries such as line strings or
  polygons). This index will be created if the feature type has a non-\ ``Point`` geometry. This
  is used to efficiently answer queries with a spatial component but no temporal component.
- **XZ3** [``xz3``] - the XZ3 index uses a three-dimensional implementation of XZ-ordering [#ref1]_ to index
  latitude, longitude, and time for non-point data. This index will be created if the feature
  type has a non-\ ``Point`` geometry and has a time attribute. This is used to efficiently
  answer queries with both spatial and temporal components.
- **Record/ID** [``id``] - the record index uses feature ID as the primary key. It is used for any query by ID.
  Additionally, certain attribute queries may end up retrieving data from the record index.
- **Attribute** [``attr``] - the attribute index uses attribute values as the primary index key. This allows for
  fast retrieval of queries without a spatio-temporal component. The attribute index includes a secondary
  spatio-temporal key that can improve queries with multiple predicates.

.. rubric:: Footnotes

.. [#ref1] Böhm, Klump, and Kriegel. "XZ-ordering: a space-filling curve for objects with spatial extension." 6th. Int. Symposium on Large Spatial Databases (SSD), 1999, Hong Kong, China. (http://www.dbs.ifi.lmu.de/Publikationen/Boehm/Ordering_99.pdf)
