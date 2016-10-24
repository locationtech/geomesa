/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex._
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
 * Iterator that operates on kryo encoded values. It will:
 *   a) evaluate the feature against an optional filter
 *   b) apply an optional transform
 *
 * Uses lazy evaluation of attributes and binary transforms when possible.
 */
class KryoLazyFilterTransformIterator extends
    SortedKeyValueIterator[Key, Value] with SamplingIterator with LazyLogging {

  import KryoLazyFilterTransformIterator._

  var source: SortedKeyValueIterator[Key, Value] = null
  val topValue: Value = new Value()

  var sft: SimpleFeatureType = null
  var filter: (SimpleFeature) => Boolean = null

  var reusableSf: KryoBufferSimpleFeature = null
  var hasTransform: Boolean = false

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {

    IteratorClassLoader.initClassLoader(getClass)

    this.source = src.deepCopy(env)
    sft = SimpleFeatureTypes.createType("test", options.get(SFT_OPT))

    val index = try { AccumuloFeatureIndex.index(options.get(INDEX_OPT)) } catch {
      case NonFatal(e) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(INDEX_OPT)}")
    }
    val kryoOptions = if (index.serializedWithId) SerializationOptions.none else SerializationOptions.withoutId
    reusableSf = new KryoFeatureSerializer(sft, kryoOptions).getReusableFeature

    val transform = Option(options.get(TRANSFORM_DEFINITIONS_OPT))
    val transformSchema = Option(options.get(TRANSFORM_SCHEMA_OPT))
    for { t <- transform; ts <- transformSchema } {
      reusableSf.setTransforms(t, SimpleFeatureTypes.createType("", ts))
    }
    hasTransform = transform.isDefined

    val cql = Option(options.get(CQL_OPT)).map(FastFilterFactory.toFilter)
    val sampling = sample(options)

    filter = (cql, sampling) match {
      case (None, None)       => (_) => true
      case (Some(c), None)    => c.evaluate
      case (None, Some(s))    => s
      case (Some(c), Some(s)) => (sf) => c.evaluate(sf) && s(sf)
    }
  }

  override def seek(range: Range, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  override def hasTop: Boolean = source.hasTop
  override def getTopKey: Key = source.getTopKey
  override def getTopValue: Value =
    if (hasTransform) {
      topValue.set(reusableSf.transform())
      topValue
    } else {
      source.getTopValue
    }

  def findTop(): Unit = {
    var found = false
    while (!found && source.hasTop) {
      reusableSf.setBuffer(source.getTopValue.get())
      if (filter(reusableSf)) {
        found = true
      } else {
        source.next()
      }
    }
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val iter = new KryoLazyFilterTransformIterator
    iter.source = source.deepCopy(env)
    iter.sft = sft
    iter.filter = filter
    iter.reusableSf = reusableSf.copy()
    iter.hasTransform = hasTransform
    iter
  }
}

object KryoLazyFilterTransformIterator {

  import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints

  val SFT_OPT                   = "sft"
  val INDEX_OPT                 = "index"
  val CQL_OPT                   = "cql"
  val TRANSFORM_SCHEMA_OPT      = "tsft"
  val TRANSFORM_DEFINITIONS_OPT = "tdefs"

  val DefaultPriority = 25

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndex,
                filter: Option[Filter],
                hints: Hints): Option[IteratorSetting] =
    configure(sft, index, filter, hints.getTransform, hints.getSampling)

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndex,
                filter: Option[Filter],
                transform: Option[(String, SimpleFeatureType)],
                sampling: Option[(Float, Option[String])],
                priority: Int = DefaultPriority): Option[IteratorSetting] = {
    if (filter.isDefined || transform.isDefined || sampling.isDefined) {
      val is = new IteratorSetting(priority, "filter-transform-iter", classOf[KryoLazyFilterTransformIterator])
      is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
      is.addOption(INDEX_OPT, index.identifier)
      filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
      transform.foreach { case (tdef, tsft) =>
        is.addOption(TRANSFORM_DEFINITIONS_OPT, tdef)
        is.addOption(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
      }
      sampling.foreach(SamplingIterator.configure(is, sft, _))
      Some(is)
    } else {
      None
    }
  }

}