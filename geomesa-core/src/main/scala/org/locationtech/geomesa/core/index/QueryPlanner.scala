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

import java.util.Map.Entry

import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.iterators.{DeDuplicatingIterator, DensityIterator}
import org.locationtech.geomesa.core.util.CloseableIterator._
import org.locationtech.geomesa.core.util.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.sort.{SortBy, SortOrder}

import scala.reflect.ClassTag

object QueryPlanner {
  val iteratorPriority_RowRegex                        = 0
  val iteratorPriority_AttributeIndexFilteringIterator = 10
  val iteratorPriority_AttributeIndexIterator          = 200
  val iteratorPriority_AttributeUniqueIterator         = 300
  val iteratorPriority_ColFRegex                       = 100
  val iteratorPriority_SpatioTemporalIterator          = 200
  val iteratorPriority_SimpleFeatureFilteringIterator  = 300
  val iteratorPriority_AnalysisIterator                = 400
}

case class QueryPlanner(schema: String,
                        featureType: SimpleFeatureType,
                        featureEncoding: FeatureEncoding) extends ExplainingLogging {
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
                  output: ExplainerOutputType = log): CloseableIterator[Entry[Key,Value]] = {

    output(s"Running ${ExplainerOutputType.toString(query)}")
    val ff = CommonFactoryFinder.getFilterFactory2
    val isDensity = query.getHints.containsKey(BBOX_KEY)
    val duplicatableData = IndexSchema.mayContainDuplicates(featureType)

    def flatten(queries: Seq[Query]): CloseableIterator[Entry[Key, Value]] =
      queries.toIterator.ciFlatMap(configureScanners(acc, sft, _, isDensity, output))

    // in some cases, where duplicates may appear in overlapping queries or the data itself, remove them
    def deduplicate(queries: Seq[Query]): CloseableIterator[Entry[Key, Value]] = {
      val flatQueries = flatten(queries)
      val decoder = SimpleFeatureDecoder(getReturnSFT(query), featureEncoding)
      new DeDuplicatingIterator(flatQueries, (key: Key, value: Value) => decoder.extractFeatureId(value))
    }

    if(isDensity) {
      val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
      val q1 = new Query(featureType.getTypeName, ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), env))
      val mixedQuery = DataUtilities.mixQueries(q1, query, "geomesa.mixed.query")
      if (duplicatableData) {
        deduplicate(Seq(mixedQuery))
      } else {
        flatten(Seq(mixedQuery))
      }
    } else {
      val rawQueries = splitQueryOnOrs(query, output)
      if (rawQueries.length > 1 || duplicatableData) {
        deduplicate(rawQueries)
      } else {
        flatten(rawQueries)
      }
    }
  }
  
  def splitQueryOnOrs(query: Query, output: ExplainerOutputType): Seq[Query] = {
    val originalFilter = query.getFilter
    output(s"Original filter: $originalFilter")

    val rewrittenFilter = rewriteFilter(originalFilter)
    output(s"Rewritten filter: $rewrittenFilter")

    val orSplitter = new OrSplittingFilter
    val splitFilters = orSplitter.visit(rewrittenFilter, null)

    // Let's just check quickly to see if we can eliminate any duplicates.
    val filters = splitFilters.distinct

    filters.map { filter =>
      val q = new Query(query)
      q.setFilter(filter)
      q
    }
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
  private def configureScanners(acc: AccumuloConnectorCreator,
                       sft: SimpleFeatureType,
                       derivedQuery: Query,
                       isDensity: Boolean,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    output(s"Transforms: ${derivedQuery.getHints.get(TRANSFORMS)}")
    val strategy = QueryStrategyDecider.chooseStrategy(acc.catalogTableFormat(sft), sft, derivedQuery)

    output(s"Strategy: ${strategy.getClass.getCanonicalName}")
    strategy.execute(acc, this, sft, derivedQuery, output)
  }

  def query(query: Query, acc: AccumuloConnectorCreator): CloseableIterator[SimpleFeature] = {
    // Perform the query
    val accumuloIterator = getIterator(acc, featureType, query)

    // Convert Accumulo results to SimpleFeatures
    adaptIterator(accumuloIterator, query)
  }

  // This function decodes/transforms that Iterator of Accumulo Key-Values into an Iterator of SimpleFeatures.
  def adaptIterator(accumuloIterator: CloseableIterator[Entry[Key,Value]], query: Query): CloseableIterator[SimpleFeature] = {
    // Perform a projecting decode of the simple feature
    val returnSFT = getReturnSFT(query)
    val decoder = SimpleFeatureDecoder(returnSFT, featureEncoding)

    // Decode according to the SFT return type.
    // if this is a density query, expand the map
    if (query.getHints.containsKey(DENSITY_KEY)) {
      accumuloIterator.flatMap { kv: Entry[Key, Value] =>
        DensityIterator.expandFeature(decoder.decode(kv.getValue))
      }
    } else {
      val features = accumuloIterator.map { kv => decoder.decode(kv.getValue) }
      if(query.getSortBy != null && query.getSortBy.length > 0) sort(features, query.getSortBy)
      else features
    }
  }

  private def sort(features: CloseableIterator[SimpleFeature],
                   sortBy: Array[SortBy]): CloseableIterator[SimpleFeature] = {
    val sortOrdering = sortBy.map {
      case SortBy.NATURAL_ORDER => Ordering.by[SimpleFeature, String](_.getID)
      case SortBy.REVERSE_ORDER => Ordering.by[SimpleFeature, String](_.getID).reverse
      case sb                   =>
        val prop = sb.getPropertyName.getPropertyName
        val ord  = attributeToComparable(prop)
        if(sb.getSortOrder == SortOrder.DESCENDING) ord.reverse
        else ord
    }
    val comp: (SimpleFeature, SimpleFeature) => Boolean =
      if(sortOrdering.length == 1) {
        // optimized case for one ordering
        val ret = sortOrdering.head
        (l, r) => ret.compare(l, r) < 0
      }  else {
        (l, r) => sortOrdering.map(_.compare(l, r)).find(_ != 0).getOrElse(0) < 0
      }
    CloseableIterator(features.toList.sortWith(comp).iterator)
  }

  def attributeToComparable[T <: Comparable[T]](prop: String)(implicit ct: ClassTag[T]): Ordering[SimpleFeature] =
      Ordering.by[SimpleFeature, T](_.getAttribute(prop).asInstanceOf[T])

  // This function calculates the SimpleFeatureType of the returned SFs.
  private def getReturnSFT(query: Query): SimpleFeatureType =
    query match {
      case _: Query if query.getHints.containsKey(DENSITY_KEY)  =>
        SimpleFeatureTypes.createType(featureType.getTypeName, DensityIterator.DENSITY_FEATURE_STRING)
      case _: Query if query.getHints.get(TRANSFORM_SCHEMA) != null =>
        query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
      case _ => featureType
    }
}

