/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.tables.RecordTable
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators.{KryoLazyStatsIterator, KryoLazyFilterTransformIterator, BinAggregatingIterator}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{Filter, Id}

import scala.collection.JavaConversions._


object RecordIdxStrategy extends StrategyProvider {

  // record searches are the least expensive as they are single row lookups (per id)
  override def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints) =
    if (QueryFilterSplitter.isFullTableScan(filter)) Int.MaxValue else 1

  def intersectIdFilters(filters: Seq[Filter]): Option[Id] = {
    if (filters.length < 2) {
      filters.headOption.map(_.asInstanceOf[Id])
    } else {
      // get the Set of IDs in *each* filter and convert to a Scala immutable Set
      // take the intersection of all sets, since the filters and joined with 'and'
      val ids = filters.map(_.asInstanceOf[Id].getIDs.map(_.toString).toSet).reduceLeft(_ intersect _)
      if (ids.nonEmpty) Some(ff.id(ids.map(ff.featureId))) else None
    }
  }
}

class RecordIdxStrategy(val filter: QueryFilter) extends Strategy with LazyLogging {

  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val ds = queryPlanner.ds
    val sft = queryPlanner.sft
    val featureEncoding = queryPlanner.ds.getFeatureEncoding(sft)
    val prefix = sft.getTableSharingPrefix

    val ranges = if (filter.primary.forall(_ == Filter.INCLUDE)) {
      // allow for full table scans - we use the record index for queries that can't be satisfied elsewhere
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
      val start = new Text(prefix)
      Seq(new aRange(start, true, aRange.followingPrefix(start), false))
    } else {
      // Multiple sets of IDs in a ID Filter are ORs. ANDs of these call for the intersection to be taken.
      // intersect together all groups of ID Filters, producing Some[Id] if the intersection returns something
      val combinedIdFilter = RecordIdxStrategy.intersectIdFilters(filter.primary)
      val identifiers = combinedIdFilter.toSeq.flatMap(_.getIdentifiers.map(_.toString))
      output(s"Extracted ID filter: ${identifiers.mkString(", ")}")
      if (identifiers.nonEmpty) {
        identifiers.map(id => aRange.exact(RecordTable.getRowKey(prefix, id)))
      } else {
        // TODO GEOMESA-347 instead pass empty query plan
        Seq.empty
      }
    }

    val table = ds.getTableName(sft.getTypeName, RecordTable)
    val threads = ds.getSuggestedThreads(sft.getTypeName, RecordTable)

    if (sft.getSchemaVersion > 5) {
      // optimized path when we know we're using kryo serialization
      if (hints.isBinQuery) {
        // use the server side aggregation
        val iters = Seq(BinAggregatingIterator.configureDynamic(sft, filter.secondary, hints, deduplicate = false))
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        BatchScanPlan(table, ranges, iters, Seq.empty, kvsToFeatures, threads, hasDuplicates = false)
      } else if (hints.isStatsIteratorQuery) {
        val iters = Seq(KryoLazyStatsIterator.configure(sft, filter.secondary, hints, deduplicate = false))
        val kvsToFeatures = KryoLazyStatsIterator.kvsToFeatures(sft)
        BatchScanPlan(table, ranges, iters, Seq.empty, kvsToFeatures, threads, hasDuplicates = false)
      } else {
        val iters = KryoLazyFilterTransformIterator.configure(sft, filter.secondary, hints).toSeq
        val kvsToFeatures = queryPlanner.defaultKVsToFeatures(hints)
        BatchScanPlan(table, ranges, iters, Seq.empty, kvsToFeatures, threads, hasDuplicates = false)
      }
    } else {
      val iters = if (filter.secondary.isDefined || hints.getTransformSchema.isDefined) {
        Seq(configureRecordTableIterator(sft, featureEncoding, filter.secondary, hints))
      } else {
        Seq.empty
      }
      val kvsToFeatures = if (hints.isBinQuery) {
        BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, hints, featureEncoding)
      } else {
        queryPlanner.defaultKVsToFeatures(hints)
      }
      BatchScanPlan(table, ranges, iters, Seq.empty, kvsToFeatures, threads, hasDuplicates = false)
    }
  }
}
