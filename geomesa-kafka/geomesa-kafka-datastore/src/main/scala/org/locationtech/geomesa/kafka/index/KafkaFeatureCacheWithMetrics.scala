/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

<<<<<<< HEAD
import com.codahale.metrics.{Gauge, Histogram}
<<<<<<< HEAD
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
=======
=======
import com.codahale.metrics.Gauge
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import com.codahale.metrics.MetricRegistry.MetricSupplier
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a9de98d0ef9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
=======
import com.codahale.metrics.MetricRegistry.MetricSupplier
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e885404c30 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
import com.codahale.metrics.MetricRegistry.MetricSupplier
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d014210d195 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae76dae421d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
import com.codahale.metrics.MetricRegistry.MetricSupplier
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 72207855ca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5c7acfc0f0e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f588ffc458 (GEOMESA-3100 Kafka layer views (#2784))
import org.locationtech.geomesa.kafka.data.KafkaDataStore.{IndexConfig, LayerView}
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.FeatureState
import org.locationtech.geomesa.kafka.index.KafkaFeatureCacheWithMetrics.SizeGauge
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics

<<<<<<< HEAD
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
=======
class KafkaFeatureCacheWithMetrics(
    sft: SimpleFeatureType,
    config: IndexConfig,
    views: Seq[LayerView],
    metrics: GeoMesaMetrics
  ) extends KafkaFeatureCacheImpl(sft, config, views) {
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))

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
