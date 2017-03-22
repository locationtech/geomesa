/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.id

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Mutation, Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFilterStrategyType
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

trait RecordQueryableIndex extends AccumuloFeatureIndex
    with IdFilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation]
    with LazyLogging {

  writable: RecordWritableIndex =>

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: AccumuloDataStore,
                            filter: AccumuloFilterStrategyType,
                            hints: Hints,
                            explain: Explainer): AccumuloQueryPlan = {
    val prefix = sft.getTableSharingPrefix

    val ranges = filter.primary match {
      case None =>
        // allow for full table scans
        filter.secondary.foreach { f =>
          logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
        }
        val start = new Text(prefix)
        Seq(new aRange(start, true, aRange.followingPrefix(start), false))

      case Some(primary) =>
        // Multiple sets of IDs in a ID Filter are ORs. ANDs of these call for the intersection to be taken.
        // intersect together all groups of ID Filters, producing a set of IDs
        val identifiers = IdFilterStrategy.intersectIdFilters(primary)
        explain(s"Extracted ID filter: ${identifiers.mkString(", ")}")
        identifiers.toSeq.map(id => aRange.exact(RecordIndex.getRowKey(prefix, id)))
    }

    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = getTableName(sft.getTypeName, ds)
      val threads = ds.config.recordThreads
      val dupes = false // record table never has duplicate entries

      val perAttributeIter = sft.getVisibilityLevel match {
        case VisibilityLevel.Feature   => Seq.empty
        case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
      }
      val (iters, kvsToFeatures, reduce) = if (hints.isBinQuery) {
        // use the server side aggregation
        val iter = BinAggregatingIterator.configureDynamic(sft, this, filter.secondary, hints, dupes)
        (Seq(iter), BinAggregatingIterator.kvsToFeatures(), None)
      } else if (hints.isDensityQuery) {
        val iter = KryoLazyDensityIterator.configure(sft, this, filter.secondary, hints, dupes)
        (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), None)
      } else if (hints.isStatsIteratorQuery) {
        val iter = KryoLazyStatsIterator.configure(sft, this, filter.secondary, hints, dupes)
        val reduce = Some(KryoLazyStatsIterator.reduceFeatures(sft, hints)(_))
        (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), reduce)
      } else if (hints.isMapAggregatingQuery) {
        val iter = KryoLazyMapAggregatingIterator.configure(sft, this, filter.secondary, hints, dupes)
        val reduce = Some(KryoLazyMapAggregatingIterator.reduceMapAggregationFeatures(hints)(_))
        (Seq(iter), entriesToFeatures(sft, hints.getReturnSft), reduce)
      } else {
        val iter = KryoLazyFilterTransformIterator.configure(sft, this, filter.secondary, hints)
        (iter.toSeq, entriesToFeatures(sft, hints.getReturnSft), None)
      }
      BatchScanPlan(filter, table, ranges, iters ++ perAttributeIter, Seq.empty, kvsToFeatures, reduce, threads, dupes)
    }
  }
}
