/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import org.locationtech.jts.geom.Geometry
import org.geotools.data.FeatureEvent
import org.geotools.data.FeatureEvent.Type
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.util.control.NonFatal

sealed abstract class KafkaFeatureEvent(source: AnyRef,
                                        eventType: FeatureEvent.Type,
                                        val time: Long,
                                        bounds: ReferencedEnvelope,
                                        filter: Filter) extends FeatureEvent(source, eventType, bounds, filter)
object KafkaFeatureEvent {

  import org.locationtech.geomesa.filter.ff

  def changed(source: SimpleFeatureSource, feature: SimpleFeature, ts: Long): KafkaFeatureEvent =
    new KafkaFeatureChanged(source, feature, ts)

  def removed(source: SimpleFeatureSource, id: String, feature: SimpleFeature, ts: Long): KafkaFeatureEvent =
    new KafkaFeatureRemoved(source, id, feature, ts)

  def cleared(source: SimpleFeatureSource, ts: Long): KafkaFeatureEvent = new KafkaFeatureCleared(source, ts)

  private def buildFilter(id: String): Filter = ff.id(new FeatureIdImpl(id))

  private def buildFilter(feature: SimpleFeature): Filter = buildFilter(feature.getID)

  private def buildBounds(feature: SimpleFeature): ReferencedEnvelope = {
    try {
      val env = feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
      ReferencedEnvelope.create(env, CRS_EPSG_4326)
    } catch {
      case NonFatal(e) => wholeWorldEnvelope
    }
  }

  class KafkaFeatureChanged(source: AnyRef, val feature: SimpleFeature, ts: Long)
      extends KafkaFeatureEvent(source, Type.CHANGED, ts, buildBounds(feature), buildFilter(feature))

  class KafkaFeatureRemoved(source: AnyRef, val id: String, val feature: SimpleFeature, ts: Long)
      extends KafkaFeatureEvent(source, Type.REMOVED, ts, buildBounds(feature),
        Option(feature).map(buildFilter).getOrElse(buildFilter(id)))

  class KafkaFeatureCleared(source: AnyRef, ts: Long)
      extends KafkaFeatureEvent(source, Type.REMOVED, ts, wholeWorldEnvelope, Filter.INCLUDE)
}
