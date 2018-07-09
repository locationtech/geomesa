.. _in_memory_index:

In-Memory Indexing
==================

The **geomesa-memory** module provides a in-memory cache of ``SimpleFeature``\ s that supports indexing and filtering,
using the `CQEngine <https://github.com/npgall/cqengine>`__ collection query engine. This is implemented by the
``GeoCQEngine`` class.

Index Configuration
-------------------

To enable an index on an attribute, add the ``cq-index`` option in the
``SimpleFeatureType`` schema::

    foo:String:cq-index=default,*geom:Point:srid=4326

The default geometry field is spatially indexed by default and does not require an option.

There are several arguments for the ``cq-index`` option that control the type of index that is built:

============= ======================= ================================================================================
Index Type    Attribute Types         Description
============= ======================= ================================================================================
``default``   Any                     Use a default type based on the attribute
``navigable`` Date and numeric types  Supports equality, greater than and less than
``radix``     String                  Supports string matching operations
``unique``    String, integer or long Supports unique values. The presence of duplicate values will cause an exception
``hash``      String, integer or long Supports equality
``none``      Any                     No index
============= ======================= ================================================================================

The default is ``none``. CQEngine will iterate over the whole collection if a suitable index is not present.

Sample usage
------------

.. code-block:: scala

    // setup
    val spec = "Who:String:cq-index=default,*Where:Point:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", spec)

    def buildFeature(sft: SimpleFeatureType, fid: Int): SimpleFeature = ...

    // create a new cache
    val cq = new GeoCQEngine(sft)

    // add a collection of features
    cq.addAll(Seq.tabulate(1000)(i => buildFeature(sft, i)))

    // add a single feature
    val feature = buildFeature(sft, 1001)
    cq.add(feature)

    // remove a single feature
    cq.remove(feature)

    // get a reader with all features that match a filter
    val f = ECQL.toFilter("Who = 'foo' AND BBOX(Where, 0, 0, 180, 90)")
    val reader = cq.getReaderForFilter(f)

    // clear the cache
    cq.clear()
