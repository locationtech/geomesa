package geomesa.core.util

import org.geotools.data.simple.SimpleFeatureCollection
import org.opengis.feature.simple.SimpleFeature

// Unsafe adapter to be only used for local memory collections
// Doesn't close the underlying iterator
class SFCIterator(sfc: SimpleFeatureCollection) extends Iterator[SimpleFeature] {
  val itr = sfc.features()

  override def hasNext: Boolean = itr.hasNext

  override def next(): SimpleFeature = itr.next
}
