.. _index_basics:

Index Basics
============

GeoMesa will create various indices for a given ``SimpleFeatureType`` schema (see :ref:`index_overview`). This
allows the execution of a variety of queries in a optimized manner. GeoMesa will make a best effort to determine
the attributes used for indexing. The attributes to use can also be specified as part of the ``SimpleFeatureType`` -
see :ref:`customizing_index_creation` for details.

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

GeoMesa will always create an ID index on ``SimpleFeature.getID()``, unless explicitly disabled.

.. _attribute_indices:

Attribute Index
---------------

Some queries are slow to answer using the default indices. For example, with twitter data you
might want to return all tweets for a given user. To speed up this type of query, any
attribute in your simple feature type may be indexed individually.

To index an attribute, add an ``index`` key to the attribute descriptor user data with a value of ``true``.

.. note::

    Accumulo data stores have an additional option to create reduced 'join' attribute indices, which can
    save space. See :ref:`accumulo_attribute_indices` for details.

.. tabs::

    .. code-tab:: java

        SimpleFeatureType sft = ...
        sft.getDescriptor("name").getUserData().put("index", "true");

    .. code-tab:: scala SchemaBuilder

        import org.locationtech.geomesa.utils.geotools.SchemaBuilder

        val sft = SchemaBuilder.builder()
            .addString("name").withIndex()
            .addDate("dtg")
            .addPoint("geom", default = true)
            .build("mySft")

Setting the user data can be done in multiple ways. See :ref:`set_sft_options` for more details.

Attribute indices also support a secondary, tiered index structure. This can improve attribute queries
that also contain a spatial and/or temporal predicate. Unless configured differently, the default geometry
and date attributes will be used to create a secondary Z3 of XZ3 index.

.. note::

  Secondary indices can only be leveraged for equality queries against the primary attribute, e.g.
  ``name = 'bob'`` can take advantage of a secondary index, but ``name ilike 'bo%'`` and ``name > 'bo'``
  cannot.

Instead of using the default, different secondary index structures can be configured by specifying the attributes
to use. To customize the secondary index, the indices must be configured through :ref:`customizing_index_creation`.
If a geometry and date attribute are specified, the secondary index will be Z3 of XZ3, as appropriate.
If just a geometry is specified, the secondary index will be Z2 of XZ2, as appropriate. If just a date
is specified, the secondary index will be an ordered temporal index.

For example, all of the following are valid ways to configure an index on a 'name' attribute, assuming
a geometry attribute named 'geom' and a date attribute named 'dtg':

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    String spec = "name:String,dtg:Date,*geom:Point:srid=4326";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);
    // enable a default z3 and a default attribute index
    sft.getUserData().put("geomesa.indices.enabled", "z3,attr:name");
    // or, enable a default z3 and an attribute index with a Z2 secondary index
    sft.getUserData().put("geomesa.indices.enabled", "z3,attr:name:geom");
    // or, enable a default z3 and an attribute index with a temporal secondary index
    sft.getUserData().put("geomesa.indices.enabled", "z3,attr:name:dtg");

To prioritize certain attributes over others, see :ref:`attribute_cardinality`.

.. warning::

  List type attributes may be indexed, but querying a list-type index may result in duplicate results. If
  duplicate results are a problem, users should implement their own de-duplication logic for list queries.

.. _index_versioning:

Index Versioning
================

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


The version numbers here may not correspond exactly to schemas created with GeoMesa versions prior to 2.3.0, as
each back-end implementation initially had its own versioning scheme. However, the implementation for each index
was consistent across back-ends in a given GeoMesa release, so if you know the GeoMesa version you can determine
the index format from the tables above. Refer to the archived `GeoMesa 2.2.0 documentation`_ to see the
back-end-specific index version numbers.

.. _GeoMesa 2.2.0 documentation: https://www.geomesa.org/documentation/2.2.0/user/datastores/index_basics.html#index-versioning

Note that GeoMesa versions prior to 1.2.2 included a geohash index. That index has been replaced with
the Z indices and is no longer supported.
