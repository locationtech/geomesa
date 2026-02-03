.. _creating_indices:

Creating Indices
================

GeoMesa will create various indices for a given ``SimpleFeatureType`` schema (see :ref:`index_overview`). This
allows the execution of a variety of queries in a optimized manner. GeoMesa will make a best effort to determine
the attributes used for indexing. The attributes to use can also be specified as part of the ``SimpleFeatureType`` -
see :ref:`specifying_indices` for details.

Spatial Index (Z2/XZ2)
----------------------

If the ``SimpleFeatureType`` has a ``Geometry``-type attribute (``Point``, ``LineString``, ``Polygon``, etc),
GeoMesa will create a spatial index on that attribute. If there is more than one ``Geometry``-type attribute,
the default one will be used. The default geometry is generally specified with a ``*`` prefix in the
``SimpleFeatureType`` string, and is the one returned by ``SimpleFeatureType.getGeometryDescriptor``.

Spatio-temporal Index (Z3/XZ3)
------------------------------

If the ``SimpleFeatureType`` has both a ``Geometry``-type attribute and a ``Date`` attribute, GeoMesa will
create a spatio-temporal index on those attributes. The ``Geometry``-type attribute used is the same as
for the spatial index, above. The ``Date`` attribute selected will be the first one declared, or can be
set explicitly. See :ref:`set_date_attribute` for details on setting the indexed date.

ID Index
--------

GeoMesa will always create an ID index on ``SimpleFeature.getID()``, unless explicitly disabled. See :ref:`specifying_indices`,
below, for details on disabling the ID index.

.. _attribute_indices:

Attribute Index
---------------

Some queries are slow to answer using the default indices. For example, with GPS data with you might want to return all records
associated with a given trip based on a trip identifier. To speed up this type of query, any attribute in your simple feature
type may be indexed individually. See :ref:`specifying_indices`, below, for details on configuring attribute indices.

.. warning::

  List type attributes may be indexed, but querying a list-type index may result in duplicate results. If
  duplicate results are a problem, users should implement their own de-duplication logic for list queries.

Attribute indices also support a secondary, tiered index structure. This can improve attribute queries
that also contain a spatial and/or temporal predicate. Unless configured differently, the default geometry
and date attributes will be used to create a secondary Z3 or XZ3 index.

.. note::

  Secondary indices can only be leveraged for equality queries against the primary attribute, e.g.
  ``name = 'bob'`` can take advantage of a secondary index, but ``name ilike 'bo%'`` and ``name > 'bo'``
  cannot.

To prioritize certain attributes over others, see :ref:`attribute_cardinality`.

.. _specifying_indices:

Specifying the Indices to Include
---------------------------------

Instead of using the default indices, you may specify the exact indices to create. This may be used to create fewer indices
(to speed up ingestion, or because you are only using certain query patterns), or to create additional indices (for example on
non-default geometries or dates).

.. warning::

    Certain queries may be much slower if you disable an index.

Indices are configured using the attribute-level user-data key ``index``. An index may be identified in several ways:

* The string ``true``, which will select an index based on the attribute type
* The string ``none`` or ``false``, which will disable any default index on the attribute
* Name of an index, e.g. ``z3`` or ``attr``, which will use the default secondary attributes for the index type
* Name of an index, plus any secondary attributes, e.g. ``z3:dtg`` or ``attr:geom:dtg``
* Name of an index, version of the index, plus any secondary attributes, e.g. ``z3:7:dtg`` or ``attr:8:geom:dtg``

Multiple indices on a single attribute may be separated with a comma (``,``).

For attribute indices, if secondary geometry and date attributes are specified, the secondary index will be Z3 or XZ3, as
appropriate. If just a geometry is specified, the secondary index will be Z2 or XZ2, as appropriate. If just a date
is specified, the secondary index will be an ordered temporal index.

The ID index does not correspond to any attribute, but it can be disabled through the feature-level user-data key
``id.index.enabled=false``.

Examples
^^^^^^^^

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

        // creates a default attribute index on name and an implicit default z3 and z2 index on geom
        String spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4325";
        // creates an attribute index on name (with a secondary date index), and a z3 index on geom and dtg
        spec = "name:String:index='attr:dtg',dtg:Date,*geom:Point:srid=4325:index='z3:dtg'";
        // creates an attribute index on name (with a secondary date index), and a z3 index on geom and dtg and disables the ID index
        spec = "name:String:index='attr:dtg',dtg:Date,*geom:Point:srid=4325:index='z3:dtg';id.index.enabled=false";

        SimpleFeatureType sft = SimpleFeatureTypes.createType("myType", spec);
        // alternatively, set user data after parsing the type string (but before calling "createSchema")
        sft.getDescriptor("name").getUserData().put("index", "true");

    .. code-tab:: scala SchemaBuilder

        import org.locationtech.geomesa.utils.geotools.SchemaBuilder

        val sft =
          SchemaBuilder.builder()
            .addString("name").withIndex("attr:dtg") // creates an attribute index on name, with a secondary date index
            .addInt("age").withIndex() // creates an attribute index on age, with a default secondary index
            .addDate("dtg") // not a primary index
            .addPoint("geom", default = true).withIndices("z3:dtg", "z2") // creates a z3 index with dtg, and a z2 index
            .userData
            .disableIdIndex() // disables the ID index
            .build("mySft")

    .. code-tab:: javascript Config

        {
          type-name = myType
          attributes = [
            { name = "name", type = "String", index = "attr:dtg" } // creates an attribute index on name, with a secondary date index
            { name = "age", type = "Int", index = "true" } // creates an attribute index on age, with a default secondary index
            { name = "dtg", type = "Date" } // not a primary index
            { name = "geom", type = "Point", srid = "4326", index = "z3:dtg,z2" } // creates a z3 index with dtg, and a z2 index
          ]
          user-data = {
            "id.index.enabled" = "false" // disables the default ID index
          }
        }

Feature-Level Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Instead of configuring individual attributes, you may set a top-level user data value in your simple feature type using the key
``geomesa.indices.enabled``. The value should contain a comma-delimited list containing a subset of index identifiers, as
specified in :ref:`index_overview` (and optionally an index version and/or list of attributes to include in the index, as
detailed above). If the ``attr`` index is specified without any attributes, then attribute-level ``index`` flags will be
examined to determine the attributes to index. Otherwise, any other attribute-level configuration will be ignored when
using ``geomesa.indices.enabled``.

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    String spec = "name:String,dtg:Date,*start:Point:srid=4326,end:Point:srid=4326";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);
    // enable a default z3 index on start + dtg
    sft.getUserData().put("geomesa.indices.enabled", "z3");
    // alternatively, enable a z3 index on start + dtg, end + dtg, and an attribute index on
    // name with a secondary index on dtg. note that this overrides the previous configuration
    sft.getUserData().put("geomesa.indices.enabled", "z3:start:dtg,z3:end:dtg,attr:name:dtg");

See :ref:`set_sft_options` for details on setting user data. If you are using the GeoMesa ``SchemaBuilder``,
you may instead call the ``indices`` method:

.. code-block:: scala

    import org.locationtech.geomesa.utils.geotools.SchemaBuilder

    val sft =
      SchemaBuilder.builder()
        .addString("name")
        .addDate("dtg")
        .addPoint("geom", default = true)
        .userData
        .indices(List("id", "z3", "attr"))
        .build("mySft")

.. _index_versioning:

Index Versioning
----------------

In order to ensure cross-compatibility, each index created by GeoMesa has a version number that identifies
the layout of data on disk, which is fixed at the time of creation. Updating GeoMesa versions
will provide bug fixes and new features, but will not update existing data to new index formats.

The exact version of an index used for each schema can be read from the ``SimpleFeatureType`` user data,
or by simply examining the name of the index tables created by GeoMesa.

The following versions are available:

.. tabs::

    .. tab:: Z3

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.1.0           Initial implementation
        2             1.2.1           Support for non-point geometries

                                      Support for shards
        3             1.2.5           Removed support for non-point geometries in favor of xz

                                      Removed redundant feature ID in row value to reduce size on disk

                                      Support for per-attribute visibility
        4             1.3.1           Support for table sharing
        5             2.0.0           Uses fixed Z-curve implementation
        6             2.3.0           Configurable attributes
        7             3.2.0           Fixes yearly epoch indexing
        ============= =============== =================================================================

    .. tab:: Z2

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.2.2           Initial implementation
        2             1.2.5           Removed support for non-point geometries in favor of xz

                                      Removed redundant feature ID in row value to reduce size on disk

                                      Support for per-attribute visibility
        3             1.3.1           Optimized deletes
        4             2.0.0           Uses fixed Z-curve implementation
        5             2.3.0           Configurable attributes
        ============= =============== =================================================================

    .. tab:: XZ3

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.2.5           Initial implementation
        2             2.3.0           Configurable attributes
        3             3.2.0           Fixes yearly epoch indexing
        ============= =============== =================================================================

    .. tab:: XZ2

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.2.5           Initial implementation
        2             2.3.0           Configurable attributes
        ============= =============== =================================================================

    .. tab:: Attribute

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.0.0           Initial implementation
        2             1.1.0           Added secondary date index
        3             1.2.5           Removed redundant feature ID in row value to reduce size on disk

                                      Support for per-attribute visibility
        4             1.3.1           Added secondary Z index
        5             1.3.2           Support for shards
        6             2.0.0-m.1       Internal row layout change
        7             2.0.0           Uses fixed Z-curve implementation
        8             2.3.0           Configurable secondary index attributes
        ============= =============== =================================================================

    .. tab:: ID

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.0.0           Initial implementation
        2             1.2.5           Removed redundant feature ID in row value to reduce size on disk

                                      Support for per-attribute visibility
        3             2.0.0           Standardized index identifier to 'id'
        4             2.3.0           Configurable attributes
        ============= =============== =================================================================


The version numbers here may not correspond exactly to schemas created with GeoMesa versions prior to 2.3.0, as each back-end
implementation initially had its own versioning scheme.

Note that GeoMesa versions prior to 1.2.2 included a geohash index. That index has been replaced with the Z indices and is no
longer supported.

Additional Index Implementations
--------------------------------

GeoMesa supports additional index formats at runtime, using
`Java service providers <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__. To add a new
index, implement ``org.locationtech.geomesa.index.api.GeoMesaFeatureIndexFactory`` and register your class under
``META-INF/services``, as described in the
`Java documentation <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__.

Once an index is registered, it can be enabled through the ``SimpleFeatureType`` user data, as described in
:ref:`specifying_indices`.

Some additional indices are provided out-of-the-box:

S2/S3 Index
^^^^^^^^^^^

The S2 and S3 indices are based on the `S2 library <https://github.com/google/s2geometry>`__:

.. pull-quote::

  The S2 library represents all data on a three-dimensional sphere (similar to a globe). This makes it
  possible to build a worldwide geographic database with no seams or singularities, using a single coordinate
  system, and with low distortion everywhere compared to the true shape of the Earth.

The S2 index is a spatial index, identified by the name ``s2``. The S3 index is a composite spatial and time index,
identified by the name ``s3``. The default time period for the S3 index can be configured through the user data
key ``geomesa.s3.interval`` (see :ref:`customizing_z_index` for reference).
