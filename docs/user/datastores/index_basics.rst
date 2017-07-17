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

To index an attribute, add an ``index`` hint to the attribute descriptor with a value of ``true``. To set
the cardinality of an attribute, use the hint ``cardinality`` with a value of ``high`` or ``low`` (see below
for a description of cardinality hints).

.. warning::

    Accumulo data stores have an additional option to create reduced 'join' attribute indices, and will
    use the reduced format by default. See :ref:`accumulo_attribute_indices` for details.

Setting the hint can be done in multiple ways. If you are using a string to indicate your simple feature type
(e.g. through the command line tools, or when using ``SimpleFeatureTypes.createType``), you can append
the hint to the attribute to be indexed, like so:

.. code-block:: java

    // append the hint after the attribute type, separated by a colon
    String spec = "name:String:index=true:cardinality=high,age:Int:index=true,dtg:Date,*geom:Point:srid=4326"
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``,
you may set the hint directly in the feature type:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getDescriptor("name").getUserData().put("index", "true");
    sft.getDescriptor("name").getUserData().put("cardinality", "high");
    sft.getDescriptor("age").getUserData().put("index", "true");

If you are using TypeSafe configuration files to define your simple feature type, you may include the hint in
the attribute field:

.. code-block:: javascript

    geomesa {
      sfts {
        "mySft" = {
          attributes = [
            { name = name, type = String, index = true, cardinality = high }
            { name = age,  type = Int,    index = true                     }
            { name = dtg,  type = Date                                     }
            { name = geom, type = Point,  srid = 4326                      }
          ]
        }
      }
    }

If you are using the GeoMesa ``SftBuilder``, you may call the overloaded attribute methods:

.. code-block:: scala

    // scala example
    import org.locationtech.geomesa.utils.geotools.SftBuilder.SftBuilder
    import org.locationtech.geomesa.utils.stats.Cardinality

    val sft = new SftBuilder()
        .stringType("name", Opts(index = true, cardinality = Cardinality.HIGH))
        .intType("age", Opts(index = true))
        .date("dtg")
        .geometry("geom", default = true)
        .build("mySft")

Cardinality Hints
^^^^^^^^^^^^^^^^^

GeoMesa has a query planner that tries to find the best strategy for answering a given query. In
general, this means using the index that will filter the result set the most, before considering
the entire query filter on the reduced data set. For simple queries, there is often only one
suitable index. However, for mixed queries, there can be multiple options.

For example, given the query ``bbox(geom, -120, -60, 120, 60) AND IN('id-01')``, we could try to
execute against the spatial index using the bounding box, or we could try to execute against the
ID index using the feature ID. In this case, we know that the ID filter will match at most one
record, while the bbox filter could match many records, so we will choose the ID index.

Attributes that are know to have many distinct values, i.e. a high cardinality, are likely to filter
out many false positives through the index structure, and thus a query against the attribute index will
touch relatively few records. Conversely, in the worst case, a Boolean attribute (for example), with only
two distinct values, would likely require scanning half of the entire data set.

Cardinality hints may be used to influence the query planner when considering attribute indices.
If an attribute is marked as having a high cardinality, the attribute index will be prioritized.
Conversely, if an attribute is marked with low cardinality, the attribute index will be de-prioritized.
