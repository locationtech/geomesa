/*
 * Copyright 2013-2014 Commonwealth Computer Research, Inc.
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

import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.FilterHelper.filterListAsAnd
import org.locationtech.geomesa.core.iterators.IteratorTrigger
import org.locationtech.geomesa.core.util.{SelfClosingBatchScanner, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{Filter, Id}

import scala.collection.JavaConverters._


object RecordIdxStrategy {
   def getRecordIdxStrategy(filter: Filter, sft: SimpleFeatureType): Option[Strategy] =
     if (filterIsId(filter)) Some(new RecordIdxStrategy) else None
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
    val featureEncoder = iqp.featureEncoder
    val keyPlanner     = IndexSchema.buildKeyPlanner(iqp.schema)
    val cfPlanner      = IndexSchema.buildColumnFamilyPlanner(iqp.schema)

    output(s"Searching the record table with filter ${query.getFilter}")

    val (idFilters, oFilters) =  partitionID(query.getFilter)
    // AND each group of filters back together into Some(filter) if they exist, or None if they do not
    val combinedIDFilter = filterListAsAnd(idFilters)

    val combinedOFilter = filterListAsAnd(oFilters)

    val identifiers = combinedIDFilter.map{_.asInstanceOf[Id].getIdentifiers.asScala.toSet}

    val rangesAsOption = identifiers.map{
      aSet => aSet.map{
        id => org.apache.accumulo.core.data.Range.exact(id.toString)
      }
    }

    val ranges = rangesAsOption match {
      case Some(filterSet) if filterSet.nonEmpty => filterSet
      case _ => throw new RuntimeException(s"Filter ${query.getFilter} results in no valid range for record table")
    }

    output(s"Extracted ID filter: ${combinedIDFilter.get}")

    output(s"Extracted Other filters: ${oFilters}")

    val qp = planQuery(ranges, output, keyPlanner, cfPlanner)

    // this should be done with care, ECQL ->Filter ->CQL is NOT a unitary transform
    val ecql = combinedOFilter.map { ECQL.toCQL }

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, featureType)

    val sffiIterCfg = getSFFIIterCfg(iteratorConfig, featureType, ecql, schema, featureEncoder, query)

    // TODO GEOMESA-322 use other strategies with density iterator
    //val topIterCfg = getTopIterCfg(query, geometryToCover, schema, featureEncoder, featureType)

    qp.copy(iterators = qp.iterators ++ List(sffiIterCfg).flatten)
  }

  def planQuery(ranges:Set[org.apache.accumulo.core.data.Range],
                output: ExplainerOutputType,
                keyPlanner: KeyPlanner,
                cfPlanner: ColumnFamilyPlanner): QueryPlan = {
    output(s"Setting ${ranges.size} ranges.")
    val iters = Seq()
    val cf = Seq()
    val accRanges = ranges.toSeq
    QueryPlan(iters, accRanges, cf)
  }
}