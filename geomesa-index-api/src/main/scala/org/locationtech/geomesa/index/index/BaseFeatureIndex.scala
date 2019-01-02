/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.conf.splitter.TableSplitter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Base feature index that consists of an optional table sharing, a configurable shard, an index key,
  * and the feature id
  */
trait BaseFeatureIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C, V, K]
    extends GeoMesaFeatureIndex[DS, F, W] with IndexAdapter[DS, F, W, R, C] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Primary key space used by this index
    *
    * @return
    */
  protected def keySpace: IndexKeySpace[V, K]

  /**
    * Strategy for sharding - defaults to using z-shard config
    *
    * @param sft simple feature type
    * @return
    */
  protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy

  /**
    * Hook to modify the scan config based on the index values extracted during range planning
    *
    * @param sft simple feature type
    * @param config base scan config
    * @param indexValues index values extracted during range planning
    * @return
    */
  protected def updateScanConfig(sft: SimpleFeatureType, config: C, indexValues: Option[V]): C = config

  override def supports(sft: SimpleFeatureType): Boolean = keySpace.supports(sft)

  override def writer(sft: SimpleFeatureType, ds: DS): F => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = shardStrategy(sft)
    val toIndexKey = keySpace.toIndexKeyBytes(sft)
    mutator(sharing, shards, toIndexKey, createInsert)
  }

  override def remover(sft: SimpleFeatureType, ds: DS): F => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = shardStrategy(sft)
    val toIndexKey = keySpace.toIndexKeyBytes(sft, lenient = true)
    mutator(sharing, shards, toIndexKey, createDelete)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String = {
    val sharing = if (sft.isTableSharing) { 1 } else { 0 }
    val shards = if (shardStrategy(sft).shards.isEmpty) { 0 } else { 1 }
    val start = sharing + shards + keySpace.indexKeyByteLength
    val idFromBytes = GeoMesaFeatureIndex.idFromBytes(sft)
    (row, offset, length, feature) => idFromBytes(row, offset + start, length - start, feature)
  }

  override def getSplits(sft: SimpleFeatureType, partition: Option[String]): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    def nonEmpty(bytes: Seq[Array[Byte]]): Seq[Array[Byte]] = if (bytes.nonEmpty) { bytes } else { Seq(Array.empty) }

    val sharing = sft.getTableSharingBytes
    val shards = nonEmpty(shardStrategy(sft).shards)

    val splits = nonEmpty(TableSplitter.getSplits(sft, name, partition))

    val result = for (shard <- shards; split <- splits) yield {
      ByteArrays.concat(sharing, shard, split)
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
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val sharing = sft.getTableSharingBytes

    val indexValues = filter.primary.map(keySpace.getIndexValues(sft, _, explain))

    val ranges = indexValues match {
      case None =>
        if (hints.getMaxFeatures.forall(_ > QueryProperties.BlockMaxThreshold.toInt.get)) {
          // check that full table scans are allowed
          QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
        }
        filter.secondary.foreach { f =>
          logger.warn(s"Running full table scan on $name index for schema ${sft.getTypeName} with filter ${filterToString(f)}")
        }
        Iterator.single(createRange(sharing, ByteArrays.rowFollowingPrefix(sharing)))

      case Some(values) =>
        val prefixes = (shardStrategy(sft).shards, sharing) match {
          case (shards, Array()) => shards
          case (Seq(), share)    => Seq(share)
          case (shards, share)   => shards.map(ByteArrays.concat(share, _))
        }
        keySpace.getRangeBytes(keySpace.getRanges(values), prefixes).map {
          case BoundedByteRange(lo, hi) => createRange(lo, hi)
          case SingleRowByteRange(row)  => createRange(row)
          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }
    }

    val useFullFilter = keySpace.useFullFilter(indexValues, Some(ds.config), hints)
    val ecql = if (useFullFilter) { filter.filter } else { filter.secondary }
    val config = updateScanConfig(sft, scanConfig(sft, ds, filter, ranges.toSeq, ecql, hints), indexValues)
    scanPlan(sft, ds, filter, config)
  }

  /**
    * Mutator for a single key space
    *
    * @param sharing table sharing bytes
    * @param shards sharding
    * @param toIndexKey function to create the primary index key
    * @param operation operation (create or delete)
    * @param feature feature to operate on
    * @return
    */
  private def mutator(sharing: Array[Byte],
                      shards: ShardStrategy,
                      toIndexKey: (Seq[Array[Byte]], SimpleFeature, Array[Byte]) => Seq[Array[Byte]],
                      operation: (Array[Byte], F) => W)
                     (feature: F): Seq[W] = {
    toIndexKey(Seq(sharing, shards(feature)), feature.feature, feature.idBytes).map(operation.apply(_, feature))
  }
}
