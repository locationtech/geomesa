/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.util.CloseableIterator._
import org.locationtech.geomesa.core.util.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeatureType

object IndexQueryPlanner {
  val iteratorPriority_RowRegex                        = 0
  val iteratorPriority_AttributeIndexFilteringIterator = 10
  val iteratorPriority_ColFRegex                       = 100
  val iteratorPriority_SpatioTemporalIterator          = 200
  val iteratorPriority_SimpleFeatureFilteringIterator  = 300
  val iteratorPriority_AnalysisIterator                = 400
}


case class IndexQueryPlanner(keyPlanner: KeyPlanner,
                             cfPlanner: ColumnFamilyPlanner,
                             schema: String,
                             featureType: SimpleFeatureType,
                             featureEncoder: SimpleFeatureEncoder) extends ExplainingLogging {
  def buildFilter(geom: Geometry, interval: Interval): KeyPlanningFilter =
    (IndexSchema.somewhere(geom), IndexSchema.somewhen(interval)) match {
      case (None, None)       =>    AcceptEverythingFilter
      case (None, Some(i))    =>
        if (i.getStart == i.getEnd) DateFilter(i.getStart)
        else                        DateRangeFilter(i.getStart, i.getEnd)
      case (Some(p), None)    =>    SpatialFilter(p)
      case (Some(p), Some(i)) =>
        if (i.getStart == i.getEnd) SpatialDateFilter(p, i.getStart)
        else                        SpatialDateRangeFilter(p, i.getStart, i.getEnd)
    }

  def netPolygon(poly: Polygon): Polygon = poly match {
    case null => null
    case p if p.covers(IndexSchema.everywhere) =>
      IndexSchema.everywhere
    case p if IndexSchema.everywhere.covers(p) => p
    case _ => poly.intersection(IndexSchema.everywhere).
      asInstanceOf[Polygon]
  }

  def netGeom(geom: Geometry): Geometry =
    Option(geom).map(_.intersection(IndexSchema.everywhere)).orNull

  def netInterval(interval: Interval): Interval = interval match {
    case null => null
    case _    => IndexSchema.everywhen.overlap(interval)
  }

  // As a pre-processing step, we examine the query/filter and split it into multiple queries.
  // TODO: Work to make the queries non-overlapping.
  def getIterator(acc: AccumuloConnectorCreator,
                  sft: SimpleFeatureType,
                  query: Query,
                  output: ExplainerOutputType = ExplainPrintln): CloseableIterator[Entry[Key,Value]] = {
    val ff = CommonFactoryFinder.getFilterFactory2
    val isDensity = query.getHints.containsKey(BBOX_KEY)
    val queries: Iterator[Query] =
      if(isDensity) {
        val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
        val q1 = new Query(featureType.getTypeName, ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), env))
        Iterator(DataUtilities.mixQueries(q1, query, "geomesa.mixed.query"))
      } else {
        splitQueryOnOrs(query, output)
      }

    queries.ciFlatMap(runQuery(acc, sft, _, isDensity, output))
  }
  
  def splitQueryOnOrs(query: Query, output: ExplainerOutputType): Iterator[Query] = {
    val originalFilter = query.getFilter
    output(s"Originalfilter is $originalFilter")

    val rewrittenFilter = rewriteFilter(originalFilter)
    output(s"Filter is rewritten as $rewrittenFilter")

    val orSplitter = new OrSplittingFilter
    val splitFilters = orSplitter.visit(rewrittenFilter, null)

    // Let's just check quickly to see if we can eliminate any duplicates.
    val filters = splitFilters.distinct

    filters.map { filter =>
      val q = new Query(query)
      q.setFilter(filter)
      q
    }.toIterator
  }

  /**
   * Helper method to execute a query against an AccumuloDataStore
   *
   * If the query contains ONLY an eligible LIKE
   * or EQUALTO query then satisfy the query with the attribute index
   * table...else use the spatio-temporal index table
   *
   * If the query is a density query use the spatio-temporal index table only
   */
  private def runQuery(acc: AccumuloConnectorCreator,
                       sft: SimpleFeatureType,
                       derivedQuery: Query,
                       isDensity: Boolean,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val filterVisitor = new FilterToAccumulo(featureType)
    val rewrittenFilter = filterVisitor.visit(derivedQuery)

    val strategy = Decider.chooseStrategy(acc, sft, derivedQuery)

    output(s"Strategy: $strategy")
    strategy.execute(acc, this, sft, derivedQuery, filterVisitor, output)
  }
}
