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

package org.locationtech.geomesa.core.index

import java.util
import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.locationtech.geomesa.core.data.tables.RecordTable
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.FilterHelper.filterListAsAnd
import org.locationtech.geomesa.core.iterators.IteratorTrigger
import org.locationtech.geomesa.core.util.{SelfClosingBatchScanner, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.identity.{FeatureId, Identifier}
import org.opengis.filter.{Filter, Id}

import scala.collection.JavaConverters._


object RecordIdxStrategy {
  def getRecordIdxStrategy(filter: Filter, sft: SimpleFeatureType): Option[Strategy] =
    if (filterIsId(filter)) Some(new RecordIdxStrategy) else None

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

  def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val recordScanner = acc.createRecordScanner(featureType)
    val qp = buildIDQueryPlan(query, iqp, featureType, output)
    configureBatchScanner(recordScanner, qp)
    SelfClosingBatchScanner(recordScanner)
  }

  def buildIDQueryPlan(query: Query,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       output: ExplainerOutputType) = {

    val schema         = iqp.schema
    val featureEncoding = iqp.featureEncoding

    output(s"Searching the record table with filter ${query.getFilter}")

    val (idFilters, oFilters) =  partitionID(query.getFilter)

    // recombine non-ID filters
    val combinedOFilter = filterListAsAnd(oFilters)

    // Multiple sets of IDs in a ID Filter are ORs. ANDs of these call for the intersection to be taken.
    // intersect together all groups of ID Filters, producing Some[Id] if the intersection returns something
    val combinedIDFilter: Option[Id] = RecordIdxStrategy.intersectIDFilters(idFilters)

    val identifiers: Option[Set[Identifier]] = combinedIDFilter.map ( _.getIdentifiers.asScala.toSet )

    val prefix = getTableSharingPrefix(featureType)

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
        // need to log a warning message as the exception will be caught by hasNext in FeatureReaderIterator
        logger.error(s"Filter ${query.getFilter} results in no valid range for record table")
        throw new RuntimeException(s"Filter ${query.getFilter} results in no valid range for record table")
    }

    output(s"Extracted ID filter: ${combinedIDFilter.get}")

    output(s"Extracted Other filters: $oFilters")

    output(s"Setting ${ranges.size} ranges.")

    val qp = QueryPlan(Seq(), ranges.toSeq, Seq())

    // this should be done with care, ECQL -> Filter -> CQL is NOT a unitary transform
    val ecql = combinedOFilter.map { ECQL.toCQL }

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, featureType)

    val sffiIterCfg = getSFFIIterCfg(iteratorConfig, featureType, ecql, schema, featureEncoding, query)

    // TODO GEOMESA-322 use other strategies with density iterator
    //val topIterCfg = getTopIterCfg(query, geometryToCover, schema, featureEncoder, featureType)

    qp.copy(iterators = qp.iterators ++ List(sffiIterCfg).flatten)
  }
}
