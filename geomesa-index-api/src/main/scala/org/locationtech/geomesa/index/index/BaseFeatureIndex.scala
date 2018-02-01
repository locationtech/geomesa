/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets

import com.google.common.primitives.Bytes
import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.TableSplitter
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.{Explainer, SplitArrays}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Base feature index that consists of an optional table sharing, a configurable shard, an index key,
  * and the feature id
  */
trait BaseFeatureIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C, K]
    extends GeoMesaFeatureIndex[DS, F, W] with IndexAdapter[DS, F, W, R, C] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  protected def keySpace: IndexKeySpace[K]

  override def supports(sft: SimpleFeatureType): Boolean = keySpace.supports(sft)

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = keySpace.toIndexKey(sft)
    (wf) => Seq(createInsert(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = keySpace.toIndexKey(sft, lenient = true)
    (wf) => Seq(createDelete(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  private def getRowKey(sharing: Array[Byte],
                        shards: IndexedSeq[Array[Byte]],
                        toIndexKey: (SimpleFeature) => Array[Byte],
                        wrapper: F): Array[Byte] = {
    val split = shards(wrapper.idHash % shards.length)
    val indexKey = toIndexKey(wrapper.feature)
    Bytes.concat(sharing, split, indexKey, wrapper.idBytes)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    val start = keySpace.indexKeyLength + (if (sft.isTableSharing) { 2 } else { 1 }) // key + table sharing + shard
    (row, offset, length) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    def nonEmpty(bytes: Seq[Array[Byte]]): Seq[Array[Byte]] = if (bytes.nonEmpty) { bytes } else { Seq(Array.empty) }

    val sharing = sft.getTableSharingBytes
    val shards = nonEmpty(SplitArrays(sft))

    val splitter = sft.getTableSplitter.getOrElse(classOf[DefaultSplitter]).newInstance().asInstanceOf[TableSplitter]
    val splits = nonEmpty(splitter.getSplits(sft, name, sft.getTableSplitterOptions))

    val result = for (shard <- shards; split <- splits) yield {
      Bytes.concat(sharing, shard, split)
    }

    // if not sharing, or the first feature in the table, drop the first split, which will otherwise be empty
    if (sharing.isEmpty || sharing.head == 0.toByte) {
      result.drop(1)
    } else {
      result
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W] = {
    val sharing = sft.getTableSharingBytes

    val indexValues = filter.primary.map(keySpace.getIndexValues(sft, _, explain))

    val ranges = indexValues match {
      case None =>
        filter.secondary.foreach { f =>
          logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
        }
        Seq(rangePrefix(sharing))

      case Some(values) =>
        val splits = SplitArrays(sft)
        val prefixes = if (sharing.length == 0) { splits } else { splits.map(Bytes.concat(sharing, _)) }
        keySpace.getRanges(sft, values).flatMap { case (s, e) =>
          prefixes.map(p => range(Bytes.concat(p, s), Bytes.concat(p, e)))
        }.toSeq
    }

    val ecql = if (useFullFilter(sft, ds, filter, indexValues, hints)) { filter.filter } else { filter.secondary }
    val config = updateScanConfig(sft, scanConfig(sft, ds, filter, ranges, ecql, hints), indexValues)
    scanPlan(sft, ds, filter, config)
  }

  protected def updateScanConfig(sft: SimpleFeatureType, config: C, indexValues: Option[K]): C = config

  protected def useFullFilter(sft: SimpleFeatureType,
                              ds: DS,
                              filter: FilterStrategy[DS, F, W],
                              indexValues: Option[K],
                              hints: Hints): Boolean
}