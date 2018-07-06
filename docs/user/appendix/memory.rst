.. _in_memory_index:

In-Memory Indexing
==================

The **geomesa-memory** module provides a in-memory cache of ``SimpleFeature``\ s that supports indexing and filtering,
using the `CQEngine <https://github.com/npgall/cqengine>`__ collection query engine. This is implemented by the
``GeoCQEngine`` class.

Index Configuration
-------------------

When creating a ``GeoCQEngine``, the attributes to index are passed in as a list of tuples in the form
``(name, type)``, where `name` corresponds to the attribute name, and `type` corresponds to the CQEngine index type:

============= ======================= ================================================================================
Index Type    Attribute Types         Description
============= ======================= ================================================================================
``default``   Any                     Choose index type based on the attribute type being indexed
``navigable`` Date and numeric types  Supports equality, greater than and less than
``radix``     String                  Supports string matching operations
``unique``    String, integer or long Supports unique values. The presence of duplicate values will cause an exception
``hash``      String, integer or long Supports equality
``geometry``  Geometries              Custom index for geometry types
============= ======================= ================================================================================

If there is no appropriate index to use for a query, the whole data set will be searched.

Sample usage
------------

.. code-block:: scala

    import org.geotools.filter.text.ecql.ECQL
    import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
    import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

    // setup
    val spec = "Who:String,*Where:Point:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", spec)

    def buildFeature(sft: SimpleFeatureType, fid: Int): SimpleFeature = ???

    // create a new cache
    val cq = new GeoCQEngine(sft, Seq(("Who", CQIndexType.DEFAULT), ("Where", CQIndexType.GEOMETRY)))

    // add a collection of features
    cq.insert(Seq.tabulate(1000)(i => buildFeature(sft, i)))

    // add a single feature
    val feature = buildFeature(sft, 1001)
    cq.insert(feature)

    // remove a single feature
    cq.remove(feature.getID)

    // get an iterator with all features that match a filter
    val filter = ECQL.toFilter("Who = 'foo' AND BBOX(Where, 0, 0, 180, 90)")
    val reader = cq.query(filter)

    // clear the cache
    cq.clear()
