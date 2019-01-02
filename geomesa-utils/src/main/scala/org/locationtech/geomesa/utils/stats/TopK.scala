/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.clearspring.StreamSummary
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.ListMap

/**
  * TopK stat
  *
  * @param sft simple feature type
  * @param property property name of the attribute to track
  * @param summary stream summary object
  * @tparam T attribute type binding
  */
class TopK[T] private [stats] (val sft: SimpleFeatureType,
                               val property: String,
                               private [stats] val summary: StreamSummary[T] = StreamSummary[T](TopK.StreamCapacity))
    extends Stat with LazyLogging {

  import TopK.StreamCapacity

  override type S = TopK[T]

  @deprecated("property")
  lazy val attribute: Int = i

  private val i = sft.indexOf(property)

  def topK(k: Int): Iterator[(T, Long)] = summary.topK(k)
  def size: Int = summary.size

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(i).asInstanceOf[T]
    if (value != null) {
      summary.offer(value)
    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(i).asInstanceOf[T]
    if (value != null) {
      summary.offer(value, -1)
    }
  }

  override def +(other: TopK[T]): TopK[T] = {
    val merged = new TopK[T](sft, property)
    merged += this
    merged += other
    merged
  }

  override def +=(other: TopK[T]): Unit =
    other.summary.topK(StreamCapacity).foreach { case (item, count) => summary.offer(item, count) }

  override def clear(): Unit = summary.clear()

  override def isEmpty: Boolean = summary.size == 0

  override def toJsonObject: Any = {
    val maps = summary.topK(10).zipWithIndex.map { case ((item, count), rank) =>
      (rank, ListMap( "value" -> item, "count" -> count))
    }
    ListMap(maps.toSeq:_*)
  }

  override def isEquivalent(other: Stat): Boolean = other match {
    case s: TopK[T] if summary.size == s.summary.size =>
      s.summary.topK(summary.size).sameElements(summary.topK(summary.size))
    case _ => false
  }
}

object TopK {
  val StreamCapacity = 1000
}
