/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait BaseTieredFeatureIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C, V, K]
    extends BaseFeatureIndex[DS, F, W, R, C, V, K] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Tiered key space beyond the primary one, if any
    *
    * @param sft simple feature type
    * @return
    */
  protected def tieredKeySpace(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]]

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    tieredKeySpace(sft) match {
      case None => super.writer(sft, ds)
      case Some(tier) =>
        val sharing = sft.getTableSharingBytes
        val shards = shardStrategy(sft)
        val toIndexKey = keySpace.toIndexKeyBytes(sft)
        val toTieredKey = tier.toIndexKeyBytes(sft)
        mutator(sharing, shards, toIndexKey, toTieredKey, createInsert)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    tieredKeySpace(sft) match {
      case None => super.remover(sft, ds)
      case Some(tier) =>
        val sharing = sft.getTableSharingBytes
        val shards = shardStrategy(sft)
        val toIndexKey = keySpace.toIndexKeyBytes(sft, lenient = true)
        val toTieredKey = tier.toIndexKeyBytes(sft, lenient = true)
        mutator(sharing, shards, toIndexKey, toTieredKey, createDelete)
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String = {
    val sharing = if (sft.isTableSharing) { 1 } else { 0 }
    val shards = if (shardStrategy(sft).shards.isEmpty) { 0 } else { 1 }
    val tieredKey = tieredKeySpace(sft).map(_.indexKeyByteLength).getOrElse(0)
    val start = sharing + shards + keySpace.indexKeyByteLength + tieredKey
    val idFromBytes = GeoMesaFeatureIndex.idFromBytes(sft)
    (row, offset, length, feature) => idFromBytes(row, offset + start, length - start, feature)
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W] = {

    val tier = tieredKeySpace(sft).orNull.asInstanceOf[IndexKeySpace[Any, Any]]
    val primary = filter.primary.orNull

    if (tier == null || primary == null) {
      // primary == null handles Filter.INCLUDE
      super.getQueryPlan(sft, ds, filter, hints, explain)
    } else {
      val values = keySpace.getIndexValues(sft, primary, explain)
      val prefixes = (shardStrategy(sft).shards, sft.getTableSharingBytes) match {
        case (shards, Array()) => shards
        case (Seq(), share)    => Seq(share)
        case (shards, share)   => shards.map(ByteArrays.concat(share, _))
      }

      val secondary = filter.secondary.orNull

      val ranges = if (secondary == null) {
        keySpace.getRangeBytes(keySpace.getRanges(values), prefixes, tier = true).map {
          case BoundedByteRange(lo, hi) => createRange(lo, ByteArrays.concat(hi, ByteRange.UnboundedUpperRange))
          case SingleRowByteRange(row)  => createRange(row, ByteArrays.concat(row, ByteRange.UnboundedUpperRange))
          case TieredByteRange(lo, hi, _, true) => createRange(lo, ByteArrays.concat(hi, ByteRange.UnboundedUpperRange))
          case TieredByteRange(lo, hi, _, false) => createRange(lo, hi)
          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }.toSeq
      } else {
        val bytes = keySpace.getRangeBytes(keySpace.getRanges(values), prefixes, tier = true).toSeq

        // only evaluate the tiered ranges if needed - depending on the primary filter we might not use them
        lazy val tiers = {
          val multiplier = math.max(1, bytes.count(_.isInstanceOf[SingleRowByteRange]))
          tier.getRangeBytes(tier.getRanges(tier.getIndexValues(sft, secondary, explain), multiplier)).toSeq
        }
        lazy val minTier = ByteRange.min(tiers)
        lazy val maxTier = ByteRange.max(tiers)

        bytes.flatMap {
          case SingleRowByteRange(row) =>
            // single row - we can use all the tiered ranges appended to the end
            if (tiers.isEmpty) {
              Iterator.single(createRange(row, ByteArrays.concat(row, ByteRange.UnboundedUpperRange)))
            } else {
              tiers.map {
                case BoundedByteRange(lo, hi) => createRange(ByteArrays.concat(row, lo), ByteArrays.concat(row, hi))
                case SingleRowByteRange(trow) => createRange(ByteArrays.concat(row, trow))
              }
            }

          case BoundedByteRange(lo, hi) =>
            // bounded ranges - we can use the min/max tier on the endpoints
            Iterator.single(createRange(ByteArrays.concat(lo, minTier), ByteArrays.concat(hi, maxTier)))

          case TieredByteRange(lo, hi, loTierable, hiTierable) =>
            // we can't use one or both of the end tiers
            val lower = if (loTierable) { ByteArrays.concat(lo, minTier) } else { lo }
            val upper = if (hiTierable) { ByteArrays.concat(hi, maxTier) } else { hi }
            Iterator.single(createRange(lower, upper))

          case r =>
            throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      }

      val useFullFilter = keySpace.useFullFilter(Some(values), Some(ds.config), hints)
      val ecql = if (useFullFilter) { filter.filter } else { filter.secondary }
      val config = updateScanConfig(sft, scanConfig(sft, ds, filter, ranges, ecql, hints), Some(values))
      scanPlan(sft, ds, filter, config)
    }
  }

  /**
    * Mutator function for two key spaces
    *
    * @param sharing table sharing bytes
    * @param shards sharding
    * @param toIndexKey function to create the primary index key
    * @param toTieredKey function to create a secondary, tiered index key
    * @param operation operation (create or delete)
    * @param feature feature to operate on
    * @return
    */
  private def mutator(sharing: Array[Byte],
                      shards: ShardStrategy,
                      toIndexKey: (Seq[Array[Byte]], SimpleFeature, Array[Byte]) => Seq[Array[Byte]],
                      toTieredKey: (Seq[Array[Byte]], SimpleFeature, Array[Byte]) => Seq[Array[Byte]],
                      operation: (Array[Byte], F) => W)
                     (feature: F): Seq[W] = {
    for (tier1 <- toIndexKey(Seq(sharing, shards(feature)), feature.feature, Array.empty);
         key   <- toTieredKey(Seq(tier1), feature.feature, feature.idBytes)) yield {
      operation.apply(key, feature)
    }
  }
}
