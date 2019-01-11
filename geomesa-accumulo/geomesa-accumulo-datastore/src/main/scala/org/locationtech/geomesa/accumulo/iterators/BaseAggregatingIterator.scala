/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => aRange, _}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.iterators.BaseAggregatingIterator.{DupeOpt, MaxDupeOpt}
import org.locationtech.geomesa.index.api.GeoMesaIndexManager
import org.locationtech.geomesa.index.iterators.AggregatingScan
import org.opengis.feature.simple.SimpleFeature

/**
 * Aggregating iterator - only works on kryo-encoded features
 */
abstract class BaseAggregatingIterator[T <: AnyRef { def isEmpty: Boolean; def clear(): Unit }]
    extends SortedKeyValueIterator[Key, Value] with AggregatingScan[T] with DeduplicatingScan[T] {

  var source: SortedKeyValueIterator[Key, Value] = _

  protected var topKey: Key = _
  private var topValue: Value = new Value()
  private var currentRange: aRange = _

  override val manager: GeoMesaIndexManager[_, _, _] = AccumuloFeatureIndex

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    import scala.collection.JavaConversions._
    this.source = src
    super.init(options.toMap)
  }

  override def hasTop: Boolean = topKey != null
  override def getTopKey: Key = topKey
  override def getTopValue: Value = topValue

  override def seek(range: aRange, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit = {
    currentRange = range
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def next(): Unit = {
    if (!source.hasTop) {
      topKey = null
      topValue = null
    } else {
      findTop()
    }
  }

  // noinspection LanguageFeature
  def findTop(): Unit = {
    val result = aggregate()
    if (result == null) {
      topKey = null // hasTop will be false
      topValue = null
    } else {
      if (topValue == null) {
        // only re-create topValue if it was nulled out
        topValue = new Value()
      }
      topValue.set(result)
    }
  }

  override def hasNextData: Boolean = source.hasTop && !currentRange.afterEndKey(source.getTopKey)

  override def nextData(setValues: (Array[Byte], Int, Int, Array[Byte], Int, Int) => Unit): Unit = {
    topKey = source.getTopKey
    val value = source.getTopValue.get()
    setValues(topKey.getRow.getBytes, 0, topKey.getRow.getLength, value, 0, value.length)
    source.next() // Advance the source iterator
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new NotImplementedError()
}

trait DeduplicatingScan[T <: AnyRef { def isEmpty: Boolean; def clear(): Unit }] extends AggregatingScan[T] {

  // server-side deduplication - not 100% effective, but we can't dedupe client side as we don't send ids
  private var dedupe: (SimpleFeature) => Boolean = _
  private val idsSeen = scala.collection.mutable.HashSet.empty[String]
  private var maxIdsToTrack = -1

  abstract override def init(options: Map[String, String]): Unit = {
    if (options.get(DupeOpt).exists(_.toBoolean)) {
      idsSeen.clear()
      maxIdsToTrack = options.get(MaxDupeOpt).map(_.toInt).getOrElse(99999)
      dedupe = deduplicate
    } else {
      dedupe = (_) => true
    }
    super.init(options)
  }

  abstract override protected def validateFeature(f: SimpleFeature): Boolean =
    dedupe(f) && super.validateFeature(f)

  private def deduplicate(sf: SimpleFeature): Boolean =
    if (idsSeen.size < maxIdsToTrack) {
      idsSeen.add(sf.getID)
    } else {
      !idsSeen.contains(sf.getID)
    }
}

object BaseAggregatingIterator extends LazyLogging {

  protected [iterators] val DupeOpt    = "dupes"
  protected [iterators] val MaxDupeOpt = "max-dupes"

  def configure(is: IteratorSetting, deduplicate: Boolean, maxDuplicates: Option[Int]): Unit = {
    is.addOption(DupeOpt, deduplicate.toString)
    maxDuplicates.foreach(m => is.addOption(MaxDupeOpt, m.toString))
  }
}
