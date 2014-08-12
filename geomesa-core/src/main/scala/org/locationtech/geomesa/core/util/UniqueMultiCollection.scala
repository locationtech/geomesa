package org.locationtech.geomesa.core.util

import collection.JavaConversions._
import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.DataFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.collection.immutable.HashMap

/**
 * Build a unique feature collection based on feature ID
 */
class UniqueMultiCollection(schema: SimpleFeatureType, collections: Iterator[Iterable[SimpleFeature]]) extends DataFeatureCollection {

  private val distinctFeatures = {
    val uniq = collection.mutable.HashMap.empty[String, SimpleFeature]
    collections.flatten.foreach { sf => uniq.put(sf.getID, sf)}
    uniq.values
  }

  override def getBounds: ReferencedEnvelope = DataUtilities.bounds(this)

  override def getCount: Int = openIterator.size
  
  override protected def openIterator = distinctFeatures.iterator

  override def toArray: Array[AnyRef] = openIterator.toArray

  override def getSchema: SimpleFeatureType = schema
}

