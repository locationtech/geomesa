/*
 * Copyright 2014-2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.index

import java.util

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data
import org.geotools.data.Query
import org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator
import org.locationtech.geomesa.accumulo.data.tables.RecordTable
import org.locationtech.geomesa.accumulo.filter._
import org.locationtech.geomesa.accumulo.index.FilterHelper.filterListAsAnd
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators.IteratorTrigger
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.identity.{FeatureId, Identifier}
import org.opengis.filter.{Filter, Id}

import scala.collection.JavaConverters._


object RecordIdxStrategy extends StrategyProvider {

  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints) =
    if (filterIsId(filter)) Some(StrategyDecision(new RecordIdxStrategy, -1)) else None

  def intersectIDFilters(filters: Seq[Filter]): Option[Id] = filters.size match {
    case 0 => None                                             // empty filter sequence
    case 1 => Some(filters.head).map ( _.asInstanceOf[Id] )    // single filter
    case _ =>                                                  // multiple filters -- need to intersect
      // get the Set of IDs in *each* filter and convert to a Scala immutable Set
      val ids = filters.map ( _.asInstanceOf[Id].getIDs.asScala.toSet )
      // take the intersection of all sets
      val intersectionIDs = ids.reduceLeft ( _ intersect _ )
      // convert back to a Option[Id]
      if (intersectionIDs.isEmpty) None
      else {
        val newIDSet: util.Set[FeatureId] = intersectionIDs.map ( x => ff.featureId(x.toString) ).asJava
        val newFilter = ff.id(newIDSet)
        Some(newFilter)
      }
    }

}

class RecordIdxStrategy extends Strategy with Logging {

  override def getQueryPlans(query: Query, queryPlanner: QueryPlanner, output: ExplainerOutputType) = {

    val sft = queryPlanner.sft
    val acc = queryPlanner.acc
    val featureEncoding = queryPlanner.featureEncoding

    output(s"Searching the record table with filter ${query.getFilter}")

    val (idFilters, oFilters) =  partitionID(query.getFilter)

    // recombine non-ID filters
    val ecql = filterListAsAnd(oFilters)

    // Multiple sets of IDs in a ID Filter are ORs. ANDs of these call for the intersection to be taken.
    // intersect together all groups of ID Filters, producing Some[Id] if the intersection returns something
    val combinedIDFilter: Option[Id] = RecordIdxStrategy.intersectIDFilters(idFilters)

    val identifiers: Option[Set[Identifier]] = combinedIDFilter.map ( _.getIdentifiers.asScala.toSet )

    val prefix = getTableSharingPrefix(sft)

    val rangesAsOption: Option[Set[data.Range]] = identifiers.map {
      aSet => aSet.map {
        id => org.apache.accumulo.core.data.Range.exact(RecordTable.getRowKey(prefix, id.toString))
      }
    }

    // check that the Set of Ranges exists and is not empty
    val ranges = rangesAsOption match {
      case Some(filterSet) if filterSet.nonEmpty => filterSet
      case _ =>
        // TODO: for below instead pass empty query plan (https://geomesa.atlassian.net/browse/GEOMESA-347)
        logger.warn(s"Filter ${query.getFilter} results in no valid range for record table")
        Seq.empty
    }

    output(s"Extracted ID filter: ${combinedIDFilter.get}")

    output(s"Extracted Other filters: $oFilters")

    output(s"Setting ${ranges.size} ranges.")

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, sft)

    val cfg = if (iteratorConfig.hasTransformOrFilter) {
      // TODO apply optimization for when transforms cover filter
      val cfg = configureRecordTableIterator(sft, featureEncoding, ecql, query)
      output(s"RecordTableIterator: ${cfg.toString }")
      Some(cfg)
    } else {
      None
    }

    // TODO GEOMESA-322 use other strategies with density iterator
    //val topIterCfg = getTopIterCfg(query, geometryToCover, schema, featureEncoder, featureType)

    val iters = Seq(cfg).flatten

    val table = acc.getRecordTable(sft)
    val threads = acc.getSuggestedRecordThreads(sft)
    val kvsToFeatures = queryPlanner.defaultKVsToFeatures(query)
    Seq(BatchScanPlan(table, ranges.toSeq, iters, Seq.empty, kvsToFeatures, threads, hasDuplicates = false))
  }
}
