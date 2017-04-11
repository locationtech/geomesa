/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable
import scala.reflect.ClassTag

case class GroupBy[T](attribute: Int,
                      exampleStat: String,
                      sft: SimpleFeatureType)(implicit ct: ClassTag[T]) extends Stat {

  override type S = GroupBy[T]

  private [stats] val groupedStats: mutable.HashMap[T, Stat] = mutable.HashMap[T, Stat]()

  def size: Int = groupedStats.size
  def get(key: T): Option[Stat] = groupedStats.get(key)
  def getOrElse[U >: Stat](key: T, default: => U = null): U = groupedStats.getOrElse(key, default)

  /**
    * Compute statistics based upon the given simple feature.
    * This method will be called for every SimpleFeature a query returns.
    *
    * @param sf feature to evaluate
    */
  override def observe(sf: SimpleFeature): Unit = {
    val key = sf.getAttribute(attribute).asInstanceOf[T]
    groupedStats.getOrElseUpdate(key, exampleStat.newCopy).observe(sf)
  }

  /**
    * Tries to remove the given simple feature from the compiled statistics.
    * Note: may not be possible to un-observe a feature, in which case this method will
    * have no effect.
    *
    * @param sf feature to un-evaluate
    */
  override def unobserve(sf: SimpleFeature): Unit = {
    val key = sf.getAttribute(attribute).asInstanceOf[T]
    groupedStats.get(key).foreach( groupedStat => groupedStat.unobserve(sf) )
  }

  /**
    * Add another stat to this stat. Avoids allocating another object.
    *
    * @param other the other stat to add
    */
  override def +=(other: GroupBy[T]): Unit = {
    other.groupedStats.map { case (key, stat) =>
      groupedStats.get(key) match {
        case Some(groupedStat) => groupedStat += stat
        case None              => groupedStats.put(key, stat.newCopy)
      }
    }
  }

  /**
    * Combine two stats into a new stat
    *
    * @param other the other stat to add
    */
  override def +(other: GroupBy[T]): GroupBy[T] = {
    val newGB = new GroupBy[T](attribute, exampleStat.newCopy)
    newGB += this
    newGB += other
    newGB
  }

  /**
    * Returns a json representation of the stat
    *
    * @return stat as a json string
    */
  override def toJson: String = {
    // TODO: Provide summary of all items in all groups together. e.g. GroupBy("cat",Count()) should have a total count as well as a per category count.
    groupedStats.map{ case (key, stat) => "{ \"" + key + "\" : " + stat.toJson + "}" }.mkString("[",",","]")
  }

  /**
    * Necessary method used by the StatIterator. Indicates if the stat has any values or not
    *
    * @return true if stat contains values
    */
  override def isEmpty: Boolean = groupedStats.values.forall(_.isEmpty)

  /**
    * Compares the two stats for equivalence. We don't use standard 'equals' as it gets messy with
    * mutable state and hash codes
    *
    * @param other other stat to compare
    * @return true if equals
    */
  override def isEquivalent(other: Stat): Boolean = {
    other match {
      case other: GroupBy[T] => !groupedStats.map{ case (key, stat) =>
          other.groupedStats.get(key).asInstanceOf[Stat].isEquivalent(stat)
        }.exists(p => p == false)
      case _ => false
    }
  }

  /**
    * Clears the stat to its original state when first initialized.
    * Necessary method used by the StatIterator.
    */
  override def clear(): Unit = groupedStats.clear()

  override def newCopy: Stat = {
    val newGB = new GroupBy(attribute, exampleStat.newCopy)
    newGB += this
    newGB
  }
}
