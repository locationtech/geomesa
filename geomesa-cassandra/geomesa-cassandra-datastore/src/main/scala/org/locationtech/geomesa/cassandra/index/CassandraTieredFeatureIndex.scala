/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import org.geotools.factory.Hints
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature, EmptyPlan, StatementPlan}
import org.locationtech.geomesa.cassandra.{CassandraFilterStrategyType, CassandraQueryPlanType, RowRange, RowValue}
import org.locationtech.geomesa.index.index.IndexKeySpace.{BoundedByteRange, SingleRowByteRange, ToIndexKeyBytes}
import org.locationtech.geomesa.index.index.{IndexKeySpace, ShardStrategy}
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait CassandraTieredFeatureIndex[T, U] extends CassandraFeatureIndex[T, U] {

  /**
    * Tiered key space beyond the primary one, if any
    *
    * @param sft simple feature type
    * @return
    */
  protected def tieredKeySpace(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]]

  /**
    * Convert a secondary scan into a c* select
    *
    * @param lower lower bound
    * @param upper upper bound
    * @return
    */
  protected def toTieredRowRange(lower: Array[Byte], upper: Array[Byte]): RowRange

  /**
    * Create values for insert with a tiered row key
    *
    * @param shards shards
    * @param toIndexKey function to create primary keys
    * @param toTieredIndexKey function to create tiered primary keys
    * @param includeFeature include the feature (for insert) or not (for delete)
    * @param cf feature
    * @return
    */
  protected def createValues(shards: ShardStrategy,
                             toIndexKey: SimpleFeature => Seq[U],
                             toTieredIndexKey: ToIndexKeyBytes,
                             includeFeature: Boolean)
                            (cf: CassandraFeature): Seq[Seq[RowValue]]

  override def writer(sft: SimpleFeatureType, ds: CassandraDataStore): CassandraFeature => Seq[Seq[RowValue]] = {
    tieredKeySpace(sft) match {
      case None => super.writer(sft, ds)
      case Some(tier) =>
        val shards = shardStrategy(sft)
        val toIndexKey = keySpace.toIndexKey(sft)
        val toTieredKey = tier.toIndexKeyBytes(sft)
        createValues(shards, toIndexKey, toTieredKey, includeFeature = true)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: CassandraDataStore): CassandraFeature => Seq[Seq[RowValue]] = {
    tieredKeySpace(sft) match {
      case None => super.remover(sft, ds)
      case Some(tier) =>
        val shards = shardStrategy(sft)
        val toIndexKey = keySpace.toIndexKey(sft, lenient = true)
        val toTieredKey = tier.toIndexKeyBytes(sft, lenient = true)
        createValues(shards, toIndexKey, toTieredKey, includeFeature = false)
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: CassandraDataStore,
                            filter: CassandraFilterStrategyType,
                            hints: Hints,
                            explain: Explainer): CassandraQueryPlanType = {
    val tier = tieredKeySpace(sft).orNull.asInstanceOf[IndexKeySpace[Any, Any]]
    val primary = filter.primary.orNull
    val secondary = filter.secondary.orNull
    lazy val tiers = tier.getRangeBytes(tier.getRanges(tier.getIndexValues(sft, secondary, explain))).map {
      case BoundedByteRange(lo, hi) => toTieredRowRange(lo, hi)
      case SingleRowByteRange(row)  => toTieredRowRange(row, row)
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }.toSeq

    if (tier == null || primary == null || secondary == null || tiers.isEmpty) {
      // primary == null handles Filter.INCLUDE
      super.getQueryPlan(sft, ds, filter, hints, explain)
    } else {
      val values = keySpace.getIndexValues(sft, primary, explain)
      val keyRanges = keySpace.getRanges(values)

      val splits = shardStrategy(sft).shards.map(b => RowRange(CassandraFeatureIndex.ShardColumn, b(0), b(0)))

      // TODO it would probably be better to make this a (range OR range) AND tieredRanges
      // instead of (range AND tieredRanges) OR (range AND tieredRanges)
      val ranges: Iterator[Seq[RowRange]] = if (splits.isEmpty) {
        keyRanges.flatMap {r =>
          val rows = toRowRanges(sft, r)
          tiers.map(t => rows.+:(t))
        }
      } else {
        keyRanges.flatMap {r =>
          val rows = toRowRanges(sft, r)
          splits.flatMap { split =>
            tiers.map(t => rows ++ Seq(split, t))
          }
        }
      }

      if (ranges.isEmpty) { EmptyPlan(filter) } else {
        import org.locationtech.geomesa.index.conf.QueryHints.RichHints

        val ks = ds.session.getLoggedKeyspace
        val tables = getTablesForQuery(sft, ds, filter.filter)

        val statements = tables.flatMap(table => ranges.map(CassandraFeatureIndex.statement(ks, table, _)))

        val useFullFilter = keySpace.useFullFilter(Some(values), Some(ds.config), hints)
        val ecql = if (useFullFilter) { filter.filter } else { filter.secondary }
        val toFeatures = resultsToFeatures(sft, ecql, hints.getTransform)
        StatementPlan(filter, tables, statements, ds.config.queryThreads, ecql, toFeatures)
      }
    }
  }
}
