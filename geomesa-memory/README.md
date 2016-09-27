# geomesa-memory

The **geomesa-memory** module provides a in-memory cache of ``SimpleFeature``s
that supports indexing and filtering, using the CQEngine collection query 
engine (https://github.com/npgall/cqengine). This is implemented by the
``GeoCQEngine`` class.

## Sample usage

```scala
    // setup
    val spec = "Who:String:cq-index=default,*Where:Point:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", spec)
    
    def buildFeature(sft: SimpleFeatureType, fid: Int): SimpleFeature = ...
    
    val feats = (0 until 1000).map(buildFeature(sft, _))
    val newfeat = buildFeature(sft, 1001)
    
    // create a new cache
    val cq = new GeoCQEngine(sft)
    
    // add a collection of features
    cq.addAll(feats)
    
    // add a single feature
    cq.add(newfeat)
    
    // remove a single feature
    cq.remove(newfeet)
    
    // clear the cache
    cq.clear()
    
    // get a FeatureReader with all features that match a filter
    val f = ECQL.toFilter("Who = 'foo' AND BBOX(Where, 0, 0, 180, 90)")
    val reader = cq.getReaderForFilter(f)
```


## Indexing

To enable an index on an attribute, add the ``cq-index`` option in the 
``SimpleFeatureType`` schema:

    foo:String:cq-index=default,*geom:Point:srid=4326

The default geometry field is geo-indexed by default and does not require
an option.

There are several arguments for the ``cq-index`` option that control
the type of index that is built:

 * ``default`` - let the module pick based on attribute type
 * ``navigable`` - indexes equality, greater than or less than; for numeric types and ``Date``
 * ``radix``- indexes for string matching operations (``String``)
 * ``unique``- indexes fields (``String``, ``Int``, ``Long``) guaranteed to have unique values
               (CQEngine will throw an exception if duplicate values are present
               for this attribute)
 * ``hash`` - use for equality on enumerable fields (``String``, ``Int``, ``Long``)
 * ``none`` - no index
 
The default is ``none``. CQEngine will iterate over the whole collection
if a suitable index is not present.
