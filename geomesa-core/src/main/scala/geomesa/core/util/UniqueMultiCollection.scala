package geomesa.core.util

import collection.JavaConversions._
import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.DataFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Build a unique feature collection based on feature ID
 */
class UniqueMultiCollection(schema: SimpleFeatureType, collections: Iterable[SimpleFeatureCollection]) extends DataFeatureCollection {

  private val distinctFeatures = {
    val tmp = collection.mutable.HashMap.empty[String, SimpleFeature]
    collections.map { c =>
      val itr = c.features
      while (itr.hasNext) {
        val sf = itr.next
        tmp.put(sf.getID, sf)
      }
    }
    tmp.toMap.values
  }

  override def getBounds: ReferencedEnvelope = DataUtilities.bounds(this)

  override def getCount: Int = openIterator.size
  
  override protected def openIterator = distinctFeatures.iterator

  override def toArray: Array[AnyRef] = openIterator.toArray

  override def getSchema: SimpleFeatureType = schema
}
