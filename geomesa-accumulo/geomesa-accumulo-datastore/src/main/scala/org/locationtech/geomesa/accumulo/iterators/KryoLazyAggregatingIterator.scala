/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => aRange, _}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

/**
 * Aggregating iterator - only works on kryo-encoded features
 */
abstract class KryoLazyAggregatingIterator[T <: AnyRef { def isEmpty: Boolean; def clear(): Unit }]
    extends SortedKeyValueIterator[Key, Value] {

  import KryoLazyAggregatingIterator._

  var sft: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key, Value] = null

  private var validate: (SimpleFeature) => Boolean = null

  // our accumulated result
  private var result: T = _

  protected var topKey: Key = null
  private var topValue: Value = new Value()
  private var currentRange: aRange = null

  private var reusableSf: KryoBufferSimpleFeature = null

  // server-side deduplication - not 100% effective, but we can't dedupe client side as we don't send ids
  private val idsSeen = scala.collection.mutable.HashSet.empty[String]
  private var maxIdsToTrack = -1

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = src.deepCopy(env)
    val options = jOptions.asScala

    sft = SimpleFeatureTypes.createType("", options(SFT_OPT))
    reusableSf = new KryoFeatureSerializer(sft).getReusableFeature
    val filt = options.get(CQL_OPT).map(FastFilterFactory.toFilter).orNull
    val dedupe = options.get(DUPE_OPT).exists(_.toBoolean)
    maxIdsToTrack = options.get(MAX_DUPE_OPT).map(_.toInt).getOrElse(99999)
    validate = (filt, dedupe) match {
      case (null, false) => (_) => true
      case (null, true)  => deduplicate
      case (_, false)    => filter(filt)
      case (_, true)     => val f = filter(filt)(_); (sf) => f(sf) && deduplicate(sf)
    }
    if (result == null) {
      result = newResult()
    }
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

  def findTop(): Unit = {
    result.clear()

    while (source.hasTop && !currentRange.afterEndKey(source.getTopKey) && notFull(result)) {
      val sf = decode(source.getTopValue.get())
      if (validate(sf)) {
        topKey = source.getTopKey
        aggregateResult(sf, result) // write the record to our aggregated results
      }
      source.next() // Advance the source iterator
    }

    if (result.isEmpty) {
      topKey = null // hasTop will be false
      topValue = null
    } else {
      if (topValue == null) {
        // only re-create topValue if it was nulled out
        topValue = new Value()
      }
      topValue.set(encodeResult(result))
    }
  }

  // hook to allow result to be chunked up
  def notFull(result: T): Boolean = true

  // hook to allow overrides in non-kryo subclasses
  def decode(value: Array[Byte]): SimpleFeature = {
    reusableSf.setBuffer(value)
    reusableSf
  }

  def newResult(): T
  def aggregateResult(sf: SimpleFeature, result: T): Unit
  def encodeResult(result: T): Array[Byte]

  def deduplicate(sf: SimpleFeature): Boolean =
    if (idsSeen.size < maxIdsToTrack) idsSeen.add(sf.getID) else !idsSeen.contains(sf.getID)

  def filter(filter: Filter)(sf: SimpleFeature): Boolean = filter.evaluate(sf)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object KryoLazyAggregatingIterator extends LazyLogging {

  // configuration keys
  protected[iterators] val SFT_OPT      = "sft"
  protected[iterators] val CQL_OPT      = "cql"
  protected[iterators] val DUPE_OPT     = "dupes"
  protected[iterators] val MAX_DUPE_OPT = "max-dupes"

  def configure(is: IteratorSetting,
                sft: SimpleFeatureType,
                filter: Option[Filter],
                deduplicate: Boolean,
                maxDuplicates: Option[Int]): Unit = {
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft))
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    is.addOption(DUPE_OPT, deduplicate.toString)
    maxDuplicates.foreach(m => is.addOption(MAX_DUPE_OPT, m.toString))
  }
}