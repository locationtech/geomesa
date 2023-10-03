/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import com.codahale.metrics.{Gauge, Histogram}
import org.locationtech.geomesa.kafka.data.KafkaDataStore.{IndexConfig, LayerView}
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.FeatureState
import org.locationtech.geomesa.kafka.index.KafkaFeatureCacheWithMetrics.SizeGauge
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

class KafkaFeatureCacheWithMetrics(
    sft: SimpleFeatureType,
    config: IndexConfig,
    views: Seq[LayerView],
    metrics: GeoMesaMetrics
  ) extends KafkaFeatureCacheImpl(sft, config, views) {

  import KafkaFeatureCacheWithMetrics.{DateMetrics, LastDateGauge}
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  metrics.gauge(sft.getTypeName, "index-size", new SizeGauge(this))

  private val updates     = metrics.meter(sft.getTypeName, "updates")
  private val removals    = metrics.meter(sft.getTypeName, "removals")
  private val expirations = metrics.meter(sft.getTypeName, "expirations")

  private val dtgMetrics = sft.getDtgIndex.map { i =>
    val last = metrics.gauge(sft.getTypeName, "dtg.latest", new LastDateGauge()).asInstanceOf[LastDateGauge]
    val latency = metrics.histogram(sft.getTypeName, "dtg.latency.millis")
    DateMetrics(i, last, latency)
  }

  override def put(feature: SimpleFeature): Unit = {
    super.put(feature)
    updates.mark()
    dtgMetrics.foreach { case DateMetrics(index, dtg, latency) =>
      val date = feature.getAttribute(index).asInstanceOf[Date]
      if (date != null) {
        dtg.setLatest(date)
        latency.update(System.currentTimeMillis() - date.getTime)
      }
    }
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

  case class DateMetrics(i: Int, last: LastDateGauge, latency: Histogram)

  class SizeGauge(cache: KafkaFeatureCache) extends Gauge[Int] {
    override def getValue: Int = cache.size()
  }

  /**
   * Tracks last date
   */
  class LastDateGauge extends Gauge[Date] {

    private val value = new AtomicLong(0)

    override def getValue: Date = new Date(value.get)

    def setLatest(value: Date): Unit = setLatest(value.getTime)

    @tailrec
    private final def setLatest(value: Long): Unit = {
      val prev = this.value.get
      if (prev < value && !this.value.compareAndSet(prev, value)) {
        setLatest(value)
      }
    }
  }

}
