/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import com.codahale.metrics.Gauge
import org.locationtech.geomesa.kafka.data.KafkaDataStore.{IndexConfig, LayerView}
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.FeatureState
import org.locationtech.geomesa.kafka.index.KafkaFeatureCacheWithMetrics.SizeGauge
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}

class KafkaFeatureCacheWithMetrics(
    sft: SimpleFeatureType,
    config: IndexConfig,
    views: Seq[LayerView],
    metrics: GeoMesaMetrics
  ) extends KafkaFeatureCacheImpl(sft, config, views) {

  metrics.gauge(sft.getTypeName, "index-size", new SizeGauge(this))

  private val updates     = metrics.meter(sft.getTypeName, "updates")
  private val removals    = metrics.meter(sft.getTypeName, "removals")
  private val expirations = metrics.meter(sft.getTypeName, "expirations")

  override def put(feature: SimpleFeature): Unit = {
    super.put(feature)
    updates.mark()
  }

  override def remove(id: String): Unit = {
    super.remove(id)
    removals.mark()
  }

  override def expire(featureState: FeatureState): Unit = {
    super.expire(featureState)
    expirations.mark()
  }
}

object KafkaFeatureCacheWithMetrics {

  class SizeGauge(cache: KafkaFeatureCache) extends Gauge[Int] {
    override def getValue: Int = cache.size()
  }
}
