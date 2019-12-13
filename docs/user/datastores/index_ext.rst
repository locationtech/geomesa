Additional Index Implementations
================================

GeoMesa supports additional index formats at runtime, using
`Java service providers <http://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__. To add a new
index, implement ``org.locationtech.geomesa.index.api.GeoMesaFeatureIndexFactory`` and register your class under
``META-INF/services``, as described in the
`Java documentation <http://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__.

Once an index is registered, it can be enabled through the ``SimpleFeatureType`` user data, as described in
:ref:`customizing_index_creation`.

Some additional indices are provided out-of-the-box:

S2/S3 Index
-----------

.. warning::

  The S2 and S3 indices are currently alpha-level software, and should be used with care. In particular,
  future compatibility support is not guaranteed.

The S2 and S3 indices are based on the `S2 library <https://s2geometry.io/>`__:

.. pull-quote::

  The S2 library represents all data on a three-dimensional sphere (similar to a globe). This makes it
  possible to build a worldwide geographic database with no seams or singularities, using a single coordinate
  system, and with low distortion everywhere compared to the true shape of the Earth.

The S2 index is a spatial index,
identified by the name ``s2``. The S3 index is a composite spatial and time index, identified by the name ``s3``.
The default time period for the S3 index can be configured through the user data key ``geomesa.s3.interval`` (see
:ref:`customizing_z_index` for reference).
