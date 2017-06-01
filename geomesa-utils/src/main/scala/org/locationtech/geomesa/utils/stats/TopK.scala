/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats
import com.clearspring.analytics.stream.StreamSummary
import com.typesafe.scalalogging.LazyLogging
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

/**
  * TopK stat
  *
  * @param attribute index of the attribute to track
  * @param summary stream summary object
  * @param ct classtag
  * @tparam T attribute type binding
  */
class TopK[T](val attribute: Int,
              private [stats] var summary: StreamSummary[T] = new StreamSummary[T](TopK.StreamCapacity))(implicit ct: ClassTag[T])
    extends Stat with LazyLogging {

  import TopK.StreamCapacity

  override type S = TopK[T]

  lazy val stringify = Stat.stringifier(ct.runtimeClass)

  def topK(k: Int): Seq[(T, Long)] = summary.topK(k).map(c => (c.getItem, c.getCount))
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
    val merged = new TopK(attribute, new StreamSummary[T](StreamCapacity))
    merged += this
    merged += other
    merged
  }

  override def +=(other: TopK[T]): Unit =
    other.summary.topK(StreamCapacity).foreach { counter =>
      if (counter.getCount > Int.MaxValue) {
        logger.warn(s"Truncating count greater than Int.MaxValue: ${counter.getCount}")
        summary.offer(counter.getItem, Int.MaxValue)
      } else {
        summary.offer(counter.getItem, counter.getCount.toInt)
      }
    }

  override def clear(): Unit = summary = new StreamSummary[T](StreamCapacity)

  override def isEmpty: Boolean = summary.size == 0

  override def toJsonObject =
    ListMap(summary.topK(10).zipWithIndex.map{ case (c,i) => (i, ListMap( "value" -> c.getItem, "count" -> c.getCount))}:_*)


  override def isEquivalent(other: Stat): Boolean = other match {
    case s: TopK[T] if summary.size == s.summary.size =>
      summary.topK(summary.size).map(c => (c.getItem, c.getCount)) ==
          s.summary.topK(summary.size).map(c => (c.getItem, c.getCount))
    case _ => false
  }
}

object TopK {
  val StreamCapacity = 1000
}
