/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.{Explainer, SplitArrays}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Base feature index that consists of an optional table sharing, a configurable shard, an index key,
  * and the feature id
  */
trait BaseFeatureIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, K]
    extends GeoMesaFeatureIndex[DS, F, W] with IndexAdapter[DS, F, W, R] with LazyLogging {

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
    val toIndexKey = keySpace.toIndexKey(sft)
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
    val splits = SplitArrays(sft).drop(1) // drop the first so we don't get an empty tablet
    if (sft.isTableSharing) {
      val sharing = sft.getTableSharingBytes
      splits.map(s => Bytes.concat(sharing, s))
    } else {
      splits
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W] = {
    val sharing = sft.getTableSharingBytes

    try {
      val ranges = filter.primary match {
        case None =>
          filter.secondary.foreach { f =>
            logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
          }
          Seq(rangePrefix(sharing))

        case Some(f) =>
          val splits = SplitArrays(sft)
          val prefixes = if (sharing.length == 0) { splits } else { splits.map(Bytes.concat(sharing, _)) }
          keySpace.getRanges(sft, f, explain).flatMap { case (s, e) =>
            prefixes.map(p => range(Bytes.concat(p, s), Bytes.concat(p, e)))
          }.toSeq
      }

      val ecql = if (useFullFilter(sft, ds, filter, hints)) { filter.filter } else { filter.secondary }
      scanPlan(sft, ds, filter, hints, ranges, ecql)
    } finally {
      keySpace.clearProcessingValues()
    }
  }

  protected def useFullFilter(sft: SimpleFeatureType, ds: DS, filter: FilterStrategy[DS, F, W], hints: Hints): Boolean
}