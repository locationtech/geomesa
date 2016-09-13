package org.locationtech.geomesa.kafka

import java.util

import KafkaFeatureEvent._
import com.vividsolutions.jts.geom.{Envelope, Point}
import org.geotools.data.FeatureEvent
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.opengis.filter.identity.FeatureId
import org.geotools.data.FeatureEvent.Type
import org.locationtech.geomesa.utils.geotools._

class KafkaFeatureEvent(source: AnyRef,
                        eventType: FeatureEvent.Type,
                        bounds: ReferencedEnvelope,
                        val feature: SimpleFeature)
  extends FeatureEvent(source, eventType, bounds, buildId(feature.getID)) { }

object KafkaFeatureEvent {
  val ff = CommonFactoryFinder.getFilterFactory2

  def buildId(id: String): Filter = {
    val fid = new FeatureIdImpl(id)
    val set = new util.HashSet[FeatureId]
    set.add(fid)

    ff.id(set)
  }

  def buildBounds(feature: SimpleFeature): ReferencedEnvelope = {
    try {
      val geom = feature.getDefaultGeometry.asInstanceOf[Point]
      val lon = geom.getX
      val lat = geom.getY

      ReferencedEnvelope.create(new Envelope(lon, lon, lat, lat), CRS_EPSG_4326)
    } catch {
      case t: Throwable => wholeWorldEnvelope
    }
  }

  def changed(src: SimpleFeatureSource, feature: SimpleFeature): FeatureEvent =
    new KafkaFeatureEvent(this,
      Type.CHANGED,
      KafkaFeatureEvent.buildBounds(feature),
      feature)

  def removed(src: SimpleFeatureSource, feature: SimpleFeature): FeatureEvent =
    new FeatureEvent(this,
      Type.REMOVED,
      KafkaFeatureEvent.buildBounds(feature),
      KafkaFeatureEvent.buildId(feature.getID))

  def cleared(src: SimpleFeatureSource): FeatureEvent =
    new FeatureEvent(this,
      Type.REMOVED,
      wholeWorldEnvelope,
      Filter.INCLUDE)
}
