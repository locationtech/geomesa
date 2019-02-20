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
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.{IteratorCache, SamplingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Iterator that operates on kryo encoded values. It will:
 *   a) evaluate the feature against an optional filter
 *   b) apply an optional transform
 *
 * Uses lazy evaluation of attributes and binary transforms when possible.
 */
class FilterTransformIterator extends SortedKeyValueIterator[Key, Value] with SamplingIterator with LazyLogging {

  import org.locationtech.geomesa.index.iterators.AggregatingScan.Configuration._

  var source: SortedKeyValueIterator[Key, Value] = _
  val topValue: Value = new Value()

  var sft: SimpleFeatureType = _
  var filter: SimpleFeature => Boolean = _

  var reusableSf: KryoBufferSimpleFeature = _
  var hasTransform: Boolean = _

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = src

    val spec = options.get(SftOpt)
    sft = IteratorCache.sft(spec)

    val index = {
      val indexSpec = options.get(IndexSftOpt)
      if (indexSpec == null) {
        IteratorCache.index(sft, spec, options.get(IndexOpt))
      } else {
        IteratorCache.index(IteratorCache.sft(indexSpec), indexSpec, options.get(IndexOpt))
      }
    }
    // noinspection ScalaDeprecation
    val kryo = if (index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }
    reusableSf = IteratorCache.serializer(spec, kryo).getReusableFeature
    reusableSf.setIdParser(index.getIdFromRow(_, _, _, null))

    val transform = Option(options.get(TransformDefsOpt))
    val transformSchema = Option(options.get(TransformSchemaOpt))
    for { t <- transform; ts <- transformSchema } {
      reusableSf.setTransforms(t, IteratorCache.sft(ts))
    }
    hasTransform = transform.isDefined

    val cql = Option(options.get(CqlOpt)).map(IteratorCache.filter(sft, spec, _))
    // TODO: can we optimize the configuration of sampling
    val sampling = sample(options)

    filter = (cql, sampling) match {
      case (None, None)       => _ => true
      case (Some(c), None)    => c.evaluate
      case (None, Some(s))    => s
      case (Some(c), Some(s)) => sf => c.evaluate(sf) && s(sf)
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
      val row = source.getTopKey.getRowData
      reusableSf.setIdBuffer(row.getBackingArray, row.offset(), row.length())

      if (filter(reusableSf)) {
        found = true
      } else {
        source.next()
      }
    }
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val iter = new FilterTransformIterator
    iter.source = source.deepCopy(env)
    iter.sft = sft
    iter.filter = filter
    iter.reusableSf = reusableSf.copy()
    iter.hasTransform = hasTransform
    iter
  }
}

object FilterTransformIterator {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.index.iterators.AggregatingScan.Configuration._

  val DefaultPriority = 25

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints): Option[IteratorSetting] =
    configure(sft, index, filter, hints.getTransform, hints.getSampling)

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints,
                priority: Int): Option[IteratorSetting] =
    configure(sft, index, filter, hints.getTransform, hints.getSampling, priority)

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                transform: Option[(String, SimpleFeatureType)],
                sampling: Option[(Float, Option[String])],
                priority: Int = DefaultPriority): Option[IteratorSetting] = {
    if (filter.isDefined || transform.isDefined || sampling.isDefined) {
      val is = new IteratorSetting(priority, "filter-transform-iter", classOf[FilterTransformIterator])
      is.addOption(SftOpt, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
      if (sft != index.sft) {
        is.addOption(IndexSftOpt, SimpleFeatureTypes.encodeType(index.sft, includeUserData = true))
      }
      is.addOption(IndexOpt, index.identifier)
      filter.foreach(f => is.addOption(CqlOpt, ECQL.toCQL(f)))
      transform.foreach { case (tdef, tsft) =>
        is.addOption(TransformDefsOpt, tdef)
        is.addOption(TransformSchemaOpt, SimpleFeatureTypes.encodeType(tsft))
      }
      sampling.foreach(SamplingIterator.configure(sft, _).foreach { case (k, v) => is.addOption(k, v) })
      Some(is)
    } else {
      None
    }
  }

}