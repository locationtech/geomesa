package geomesa.core.util

import collection.JavaConversions._
import org.geotools.data.DataUtilities
import org.geotools.data.simple.{SimpleFeatureIterator, SimpleFeatureCollection}
import org.geotools.data.store.DataFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}

class MultiCollection(schema: SimpleFeatureType, collections: Iterable[SimpleFeatureCollection]) extends DataFeatureCollection {

  override def getBounds: ReferencedEnvelope = DataUtilities.bounds(this)

  override def getCount: Int = openIterator.size
  
  override protected def openIterator = new MultipleCollectionsIterator
  
  class MultipleCollectionsIterator extends java.util.Iterator[SimpleFeature] {
    val collectionsItr = collections.iterator
    var curItr: SimpleFeatureIterator = null
    
    override def hasNext: Boolean = {
      if(curItr != null && curItr.hasNext) return true
      
      while(collectionsItr.hasNext) {
        if(curItr != null) curItr.close
        curItr = collectionsItr.next.features
        if(curItr.hasNext) return true
      }
      
      if(curItr != null) curItr.close()
      
      false
    }
    
    override def remove = throw new UnsupportedOperationException("Not supported")

    override def next: SimpleFeature = curItr.next
  }

  override def toArray: Array[AnyRef] = openIterator.toArray

  override def getSchema: SimpleFeatureType = schema
}
