/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.id

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Mutation, Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex._
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

trait RecordQueryableIndex extends AccumuloFeatureIndex
    with IdFilterStrategy[AccumuloDataStore, WritableFeature, Seq[Mutation], QueryPlan]
    with LazyLogging {

  writable: AccumuloWritableIndex =>

  override def getQueryPlan(sft: SimpleFeatureType,
                            ops: AccumuloDataStore,
                            filter: AccumuloFilterStrategy,
                            hints: Hints,
                            explain: Explainer): QueryPlan = {
    val featureEncoding = ops.getFeatureEncoding(sft)
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
      val table = ops.getTableName(sft.getTypeName, this)
      val threads = ops.getSuggestedThreads(sft.getTypeName, this)
      val dupes = false // record table never has duplicate entries

      val perAttributeIter = sft.getVisibilityLevel match {
        case VisibilityLevel.Feature   => Seq.empty
        case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
      }
      val (iters, kvsToFeatures) = if (hints.isBinQuery) {
        // use the server side aggregation
        val iter = BinAggregatingIterator.configureDynamic(sft, this, filter.secondary, hints, dupes)
        (Seq(iter), BinAggregatingIterator.kvsToFeatures())
      } else if (hints.isDensityQuery) {
        val iter = KryoLazyDensityIterator.configure(sft, this, filter.secondary, hints)
        (Seq(iter), KryoLazyDensityIterator.kvsToFeatures())
      } else if (hints.isStatsIteratorQuery) {
        val iter = KryoLazyStatsIterator.configure(sft, this, filter.secondary, hints, dupes)
        (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft))
      } else {
        val iter = KryoLazyFilterTransformIterator.configure(sft, this, filter.secondary, hints)
        (iter.toSeq, entriesToFeatures(sft, hints.getReturnSft))
      }
      BatchScanPlan(filter, table, ranges, iters ++ perAttributeIter, Seq.empty, kvsToFeatures, threads, dupes)
    }
  }
}
