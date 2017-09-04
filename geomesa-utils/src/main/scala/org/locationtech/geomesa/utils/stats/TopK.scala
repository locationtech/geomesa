/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.clearspring.StreamSummary
import org.opengis.feature.simple.SimpleFeature

import scala.collection.immutable.ListMap

/**
  * TopK stat
  *
  * @param attribute index of the attribute to track
  * @param summary stream summary object
  * @tparam T attribute type binding
  */
class TopK[T](val attribute: Int, private [stats] val summary: StreamSummary[T] = StreamSummary[T](TopK.StreamCapacity))
    extends Stat with LazyLogging {

  import TopK.StreamCapacity

  override type S = TopK[T]

  def topK(k: Int): Iterator[(T, Long)] = summary.topK(k)
  def size: Int = summary.size

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      summary.offer(value)
    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      summary.offer(value, -1)
    }
  }

  override def +(other: TopK[T]): TopK[T] = {
    val merged = new TopK[T](attribute)
    merged += this
    merged += other
    merged
  }

  override def +=(other: TopK[T]): Unit =
    other.summary.topK(StreamCapacity).foreach { case (item, count) => summary.offer(item, count) }

  override def clear(): Unit = summary.clear()

  override def isEmpty: Boolean = summary.size == 0

  override def toJsonObject: Any = {
    val maps = summary.topK(10).zipWithIndex.map { case ((item, count), i) =>
      (i, ListMap( "value" -> item, "count" -> count))
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
