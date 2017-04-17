/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators
import java.io.ByteArrayOutputStream
import java.util.Map.Entry

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.iterators.KryoLazyAggregatingIterator.SFT_OPT
import org.locationtech.geomesa.accumulo.iterators.KryoLazyFilterTransformIterator.{TRANSFORM_DEFINITIONS_OPT, TRANSFORM_SCHEMA_OPT}
import org.locationtech.geomesa.arrow.ArrowEncodedSft
import org.locationtech.geomesa.arrow.io.DictionaryBuildingWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ArrowFileIterator extends KryoLazyAggregatingIterator[ArrowFileAggregate] with SamplingIterator {

  import ArrowFileIterator.{BatchSizeKey, DictionaryKey, IncludeFidsKey, aggregateCache}

  var aggregate: (SimpleFeature, ArrowFileAggregate) => Unit = _
  var underBatchSize: (ArrowFileAggregate) => Boolean = _

  override def init(options: Map[String, String]): ArrowFileAggregate = {
    underBatchSize = options.get(BatchSizeKey).map(_.toInt) match {
      case None    => (_) => true
      case Some(i) => (a) => a.size < i
    }
    val encodedDictionaries = options(DictionaryKey)
    val dictionaries = encodedDictionaries.split(",")
    val includeFids = options(IncludeFidsKey).toBoolean
    val (arrowSft, arrowSftString) =
      if (hasTransform) { (transformSft, options(TRANSFORM_SCHEMA_OPT)) } else { (sft, options(SFT_OPT)) }
    aggregate = sample(options) match {
      case None       => (sf, result) => result.add(sf)
      case Some(samp) => (sf, result) => if (samp(sf)) { result.add(sf) }
    }
    aggregateCache.getOrElseUpdate(arrowSftString + includeFids + encodedDictionaries,
      new ArrowFileAggregate(arrowSft, dictionaries, includeFids))
  }

  override def notFull(result: ArrowFileAggregate): Boolean = underBatchSize(result)

  override def aggregateResult(sf: SimpleFeature, result: ArrowFileAggregate): Unit = aggregate(sf, result)

  override def encodeResult(result: ArrowFileAggregate): Array[Byte] = result.encode()
}

class ArrowFileAggregate(sft: SimpleFeatureType, dictionaries: Seq[String], includeFids: Boolean) {

  import org.locationtech.geomesa.arrow.allocator

  private val writer = DictionaryBuildingWriter.create(sft, dictionaries, includeFids, GeometryPrecision.Float)
  private val os = new ByteArrayOutputStream()

  def add(sf: SimpleFeature): Unit = writer.add(sf)

  def isEmpty: Boolean = writer.size == 0

  def size: Int = writer.size

  def clear(): Unit = writer.clear()

  def encode(): Array[Byte] = {
    os.reset()
    writer.encode(os)
    os.toByteArray
  }
}

object ArrowFileIterator {

  private val BatchSizeKey   = "grp"
  private val DictionaryKey  = "dict"
  private val IncludeFidsKey = "fids"

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowFileAggregate]

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                dictionaries: Seq[String],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = ArrowBatchIterator.DefaultPriority): IteratorSetting = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val is = new IteratorSetting(priority, "arrow-iter", classOf[ArrowFileIterator])
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    hints.getSampling.foreach(SamplingIterator.configure(is, sft, _))
    hints.getArrowBatchSize.foreach(i => is.addOption(BatchSizeKey, i.toString))
    is.addOption(DictionaryKey, dictionaries.mkString)
    is.addOption(IncludeFidsKey, hints.isArrowIncludeFid.toString)
    hints.getTransform.foreach { case (tdef, tsft) =>
      is.addOption(TRANSFORM_DEFINITIONS_OPT, tdef)
      is.addOption(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
    }
    is
  }

  /**
    * Adapts the iterator to create simple features.
    * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
    */
  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", ArrowEncodedSft)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    (e: Entry[Key, Value]) => {
      sf.setAttribute(0, e.getValue.get())
      sf
    }
  }
}
