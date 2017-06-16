/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.io.ByteArrayOutputStream

import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.DictionaryBuildingWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter


trait ArrowFileScan extends AggregatingScan[ArrowFileAggregate] {

  private var batchSize: Int = _

  override def initResult(sft: SimpleFeatureType,
                          transform: Option[SimpleFeatureType],
                          options: Map[String, String]): ArrowFileAggregate = {
    import AggregatingScan.Configuration.{SftOpt, TransformSchemaOpt}
    import ArrowFileScan.Configuration.{BatchSizeKey, DictionaryKey, IncludeFidsKey}
    import ArrowFileScan.aggregateCache

    batchSize = options(BatchSizeKey).toInt
    val encodedDictionaries = options(DictionaryKey)
    val dictionaries = encodedDictionaries.split(",")
    val encoding = SimpleFeatureEncoding.min(options(IncludeFidsKey).toBoolean)
    val (arrowSft, arrowSftString) = transform match {
      case Some(tsft) => (tsft, options(TransformSchemaOpt))
      case None       => (sft, options(SftOpt))
    }
    aggregateCache.getOrElseUpdate(arrowSftString + encoding + encodedDictionaries,
      new ArrowFileAggregate(arrowSft, dictionaries, encoding))
  }

  override def notFull(result: ArrowFileAggregate): Boolean = result.size < batchSize

  override def aggregateResult(sf: SimpleFeature, result: ArrowFileAggregate): Unit = result.add(sf)

  override def encodeResult(result: ArrowFileAggregate): Array[Byte] = result.encode()
}

class ArrowFileAggregate(sft: SimpleFeatureType, dictionaries: Seq[String], encoding: SimpleFeatureEncoding) {

  import org.locationtech.geomesa.arrow.allocator

  private val writer = DictionaryBuildingWriter.create(sft, dictionaries, encoding)
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

object ArrowFileScan {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  object Configuration {
    val BatchSizeKey   = "batch"
    val DictionaryKey  = "dict"
    val IncludeFidsKey = "fids"
  }

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowFileAggregate]

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                dictionaries: Seq[String],
                hints: Hints): Map[String, String] = {
    import Configuration.{BatchSizeKey, DictionaryKey, IncludeFidsKey}
    AggregatingScan.configure(sft, index, filter, hints.getTransform, hints.getSampling) ++ Map(
      BatchSizeKey -> hints.getArrowBatchSize.map(_.toString).getOrElse(ArrowProperties.BatchSize.get),
      DictionaryKey -> dictionaries.mkString,
      IncludeFidsKey -> hints.isArrowIncludeFid.toString
    )
  }
}
