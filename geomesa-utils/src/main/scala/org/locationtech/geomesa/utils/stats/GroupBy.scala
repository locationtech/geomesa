/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.reflect.ClassTag

class GroupBy[T] private [stats] (val sft: SimpleFeatureType,
                                  val property: String,
                                  val stat: String)
                                 (implicit val ct: ClassTag[T]) extends Stat {

  override type S = GroupBy[T]

  @deprecated("property")
  lazy val attribute: Int = i
  @deprecated("stat")
  lazy val exampleStat: String = stat

  private val i = sft.indexOf(property)
  private [stats] val groups = scala.collection.mutable.Map.empty[T, Stat]

  def size: Int = groups.size
  def get(key: T): Option[Stat] = groups.get(key)
  def getOrElse[U >: Stat](key: T, default: => U): U = groups.getOrElse(key, default)

  private def buildNewStat: Stat = StatParser.parse(sft, stat)

  /**
    * Compute statistics based upon the given simple feature.
    * This method will be called for every SimpleFeature a query returns.
    *
    * @param sf feature to evaluate
    */
  override def observe(sf: SimpleFeature): Unit = {
    val key = sf.getAttribute(i).asInstanceOf[T]
    groups.getOrElseUpdate(key, buildNewStat).observe(sf)
  }

  /**
    * Tries to remove the given simple feature from the compiled statistics.
    * Note: may not be possible to un-observe a feature, in which case this method will
    * have no effect.
    *
    * @param sf feature to un-evaluate
    */
  override def unobserve(sf: SimpleFeature): Unit = {
    val key = sf.getAttribute(i).asInstanceOf[T]
    groups.get(key).foreach(groupedStat => groupedStat.unobserve(sf))
  }

  /**
    * Add another stat to this stat. Avoids allocating another object.
    *
    * @param other the other stat to add
    */
  override def +=(other: GroupBy[T]): Unit = {
    other.groups.foreach { case (key, s) =>
      groups.getOrElseUpdate(key, buildNewStat) += s
    }
  }

  /**
    * Combine two stats into a new stat
    *
    * @param other the other stat to add
    */
  override def +(other: GroupBy[T]): GroupBy[T] = {
    val sum = new GroupBy[T](sft, property, stat)
    sum += this
    sum += other
    sum
  }

  override def toJsonObject: Seq[Map[T, Any]] = {
    val keyClass = groups.keys.headOption.map(_.getClass).getOrElse(ct.runtimeClass)
    if (classOf[Comparable[T]].isAssignableFrom(keyClass)) {
      val ordering = new Ordering[T] {
        def compare(l: T, r: T): Int = l.asInstanceOf[Comparable[T]].compareTo(r)
      }
      groups.toSeq.sortBy(_._1)(ordering)
    } else {
      groups.toSeq
    }
  }.map { case (k, v) => Map(k -> v.toJsonObject) }


  /**
    * Necessary method used by the StatIterator. Indicates if the stat has any values or not
    *
    * @return true if stat contains values
    */
  override def isEmpty: Boolean = groups.values.forall(_.isEmpty)

  /**
    * Compares the two stats for equivalence. We don't use standard 'equals' as it gets messy with
    * mutable state and hash codes
    *
    * @param other other stat to compare
    * @return true if equals
    */
  override def isEquivalent(other: Stat): Boolean = {
    other match {
      case other: GroupBy[T] =>
        groups.keys == other.groups.keys &&
          groups.forall { case (key, s) => other.groups(key).isEquivalent(s) }
      case _ => false
    }
  }

  /**
    * Clears the stat to its original state when first initialized.
    * Necessary method used by the StatIterator.
    */
  override def clear(): Unit = groups.clear()
}
