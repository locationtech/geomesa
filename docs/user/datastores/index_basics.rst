Index Basics
============

GeoMesa will create various indices for a given ``SimpleFeatureType`` schema (see :ref:`index_overview`). This
allows us to answer a variety of queries in a optimized manner. GeoMesa will make a best effort to determine
the attributes used for indexing. The attributes to use can also be specified as part of the ``SimpleFeatureType``.

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

GeoMesa will always create an ID index on ``SimpleFeature.getID()``.

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

.. code-block:: java

    SimpleFeatureType sft = ...
    sft.getDescriptor("name").getUserData().put("index", "true");

If you are using the GeoMesa ``SftBuilder``, you may call the overloaded attribute methods:

.. code-block:: scala

    // scala example
    import org.locationtech.geomesa.utils.geotools.SftBuilder.SftBuilder

    val sft = new SftBuilder()
        .stringType("name", Opts(index = true)
        .date("dtg")
        .geometry("geom", default = true)
        .build("mySft")

Setting the user data can be done in multiple ways. See :ref:`set_sft_options` for more details.

To prioritize certain attributes over others, see :ref:`attribute_cardinality`.

.. _index_versioning:

Index Versioning
================

In order to ensure cross-compatibility, each index created by GeoMesa has a version number that identifies
the layout of data on disk, which is fixed at the time of creation. Updating GeoMesa versions
will provide bug fixes and new features, but will not update existing data to new index formats.

The exact version of an index used for each schema can be read from the ``SimpleFeatureType`` user data,
or by simple examining the name of the index tables created by GeoMesa. The particular
version numbers vary across the different back-end implementations. See :ref:`accumulo_index_versions`,
:ref:`hbase_index_versions` and :ref:`cassandra_index_versions` for the differences between index versions.
