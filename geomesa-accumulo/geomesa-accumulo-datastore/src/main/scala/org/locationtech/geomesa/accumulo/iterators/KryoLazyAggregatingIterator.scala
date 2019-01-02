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
import org.apache.hadoop.io.Text
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Aggregating iterator - only works on kryo-encoded features
 */
abstract class KryoLazyAggregatingIterator[T <: AnyRef { def isEmpty: Boolean; def clear(): Unit }]
    extends SortedKeyValueIterator[Key, Value] {

  import KryoLazyAggregatingIterator._

  var sft: SimpleFeatureType = _
  var transformSft: SimpleFeatureType = _
  var index: AccumuloFeatureIndex = _
  var source: SortedKeyValueIterator[Key, Value] = _

  private var validate: () => Boolean = _

  // our accumulated result
  private var result: T = _

  protected var topKey: Key = _
  private var topValue: Value = new Value()
  private var currentRange: aRange = _

  private var reusableSf: KryoBufferSimpleFeature = _
  private var reusableTransformSf: TransformSimpleFeature = _
  private var getId: (Text, SimpleFeature) => String = _
  var hasTransform: Boolean = _

  // server-side deduplication - not 100% effective, but we can't dedupe client side as we don't send ids
  private val idsSeen = scala.collection.mutable.HashSet.empty[String]
  private var maxIdsToTrack = -1

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = src
    val options = jOptions.asScala

    val spec = options(SFT_OPT)
    sft = IteratorCache.sft(spec)

    index = try { AccumuloFeatureIndex.index(options(INDEX_OPT)) } catch {
      case NonFatal(_) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(INDEX_OPT)}")
    }

    // noinspection ScalaDeprecation
    if (index.serializedWithId) {
      reusableSf = IteratorCache.serializer(spec, SerializationOptions.none).getReusableFeature
      getId = (_, _) => reusableSf.getID
    } else {
      val getIdFromRow = index.getIdFromRow(sft)
      reusableSf = IteratorCache.serializer(spec, SerializationOptions.withoutId).getReusableFeature
      getId = (row, sf) => getIdFromRow(row.getBytes, 0, row.getLength, sf)
    }

    import org.locationtech.geomesa.accumulo.iterators.KryoLazyFilterTransformIterator._
    val transform = Option(jOptions.get(TRANSFORM_DEFINITIONS_OPT))
    val transformSchema = Option(jOptions.get(TRANSFORM_SCHEMA_OPT))
    for { t <- transform; ts <- transformSchema } {
      transformSft = IteratorCache.sft(ts)
      reusableTransformSf = TransformSimpleFeature(IteratorCache.sft(spec), transformSft, t)
    }
    hasTransform = transform.isDefined

    val filt = options.get(CQL_OPT).map(IteratorCache.filter(sft, spec, _)).orNull
    val dedupe = options.get(DUPE_OPT).exists(_.toBoolean)
    maxIdsToTrack = options.get(MAX_DUPE_OPT).map(_.toInt).getOrElse(99999)
    idsSeen.clear()
    validate = (filt, dedupe) match {
      case (null, false) => () => true
      case (null, true)  => deduplicate
      case (_, false)    => filter(filt)
      case (_, true)     => () => filter(filt) && deduplicate
    }
    result = init(options.toMap)
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
    result.clear()

    while (source.hasTop && !currentRange.afterEndKey(source.getTopKey) && notFull(result)) {
      reusableSf.setBuffer(source.getTopValue.get())
      reusableSf.setId(getId(source.getTopKey.getRow, reusableSf))
      if (validate()) {
        topKey = source.getTopKey

        // write the record to our aggregated results
        if (hasTransform) {
          reusableTransformSf.setFeature(reusableSf)
          aggregateResult(reusableTransformSf, result)
        } else {
          aggregateResult(reusableSf, result)
        }
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

  def init(options: Map[String, String]): T
  def aggregateResult(sf: SimpleFeature, result: T): Unit
  def encodeResult(result: T): Array[Byte]

  def deduplicate(): Boolean =
    if (idsSeen.size < maxIdsToTrack) {
      idsSeen.add(reusableSf.getID)
    } else {
      !idsSeen.contains(reusableSf.getID)
    }

  def filter(filter: Filter)(): Boolean = filter.evaluate(reusableSf)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new NotImplementedError()
}

object KryoLazyAggregatingIterator extends LazyLogging {

  // configuration keys
  protected[iterators] val SFT_OPT      = "sft"
  protected[iterators] val CQL_OPT      = "cql"
  protected[iterators] val DUPE_OPT     = "dupes"
  protected[iterators] val MAX_DUPE_OPT = "max-dupes"
  protected[iterators] val INDEX_OPT    = "index"


  def configure(is: IteratorSetting,
                sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                deduplicate: Boolean,
                maxDuplicates: Option[Int]): Unit = {
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    is.addOption(DUPE_OPT, deduplicate.toString)
    maxDuplicates.foreach(m => is.addOption(MAX_DUPE_OPT, m.toString))
    is.addOption(INDEX_OPT, index.identifier)
  }
}
