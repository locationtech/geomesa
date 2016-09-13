package org.locationtech.geomesa.kafka

import org.geotools.data.FilteringFeatureReader
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.{DFR, DFI, FR}
import org.locationtech.geomesa.utils.index.QuadTreeFeatureStore
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.spatial.{BBOX, Intersects, Within}

import scala.collection.JavaConversions._
import scala.collection.mutable

trait KafkaConsumerFeatureCache extends QuadTreeFeatureStore {

  def features: mutable.Map[String, FeatureHolder]

  private val ff = CommonFactoryFinder.getFilterFactory2

  // optimized for filter.include
  def size(f: Filter): Int = {
    if (f == Filter.INCLUDE) {
      features.size
    } else {
      getReaderForFilter(f).getIterator.length
    }
  }

  def size(): Int = {
    features.size
  }

  def getReaderForFilter(filter: Filter): FR =
    filter match {
      case f: IncludeFilter => include(f)
      case f: Id => fid(f)
      case f: And => and(f)
      case f: BBOX => bbox(f)
      case f: Intersects => intersects(f)
      case f: Within => within(f)
      case f: Or => or(f)
      case f => unoptimized(f)
    }

  def include(i: IncludeFilter) = new DFR(sft, new DFI(features.valuesIterator.map(_.sf)))

  def fid(ids: Id): FR = {
    val iter = ids.getIDs.flatMap(id => features.get(id.toString).map(_.sf)).iterator
    new DFR(sft, new DFI(iter))
  }

  def and(a: And): FR = {
    val geometries = extractGeometries(a, sft.getGeometryDescriptor.getLocalName)
    if (geometries.isEmpty) {
      unoptimized(a)
    } else {
      val envelope = geometries.head.getEnvelopeInternal
      geometries.tail.foreach(g => envelope.expandToInclude(g.getEnvelopeInternal))
      new DFR(sft, new DFI(spatialIndex.query(envelope, a.evaluate)))
    }
  }

  def or(o: Or): FR = {
    val readers = o.getChildren.map(getReaderForFilter).map(_.getIterator)
    val composed = readers.foldLeft(Iterator[SimpleFeature]())(_ ++ _)
    new DFR(sft, new DFI(composed))
  }

  def unoptimized(f: Filter): FR =
    new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](include(Filter.INCLUDE), f)

}

