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

import java.util.Map.Entry
import java.util.{Map => JMap}

import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.{DataUtilities, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.filter._
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanners.FeatureFunction
import org.locationtech.geomesa.accumulo.iterators.TemporalDensityIterator._
import org.locationtech.geomesa.accumulo.iterators.{DeDuplicatingIterator, DensityIterator, MapAggregatingIterator, TemporalDensityIterator}
import org.locationtech.geomesa.accumulo.sumNumericValueMutableMaps
import org.locationtech.geomesa.accumulo.util.CloseableIterator
import org.locationtech.geomesa.accumulo.util.CloseableIterator._
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features._
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.sort.{SortBy, SortOrder}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Executes a query against geomesa
 */
case class QueryPlanner(sft: SimpleFeatureType,
                        featureEncoding: SerializationType,
                        stSchema: String,
                        acc: AccumuloConnectorCreator,
                        hints: StrategyHints,
                        version: Int) extends ExplainingLogging with IndexFilterHelpers with MethodProfiling {

  import org.locationtech.geomesa.accumulo.index.QueryPlanner._

  val featureEncoder = SimpleFeatureSerializers(sft, featureEncoding)
  val featureDecoder = SimpleFeatureDeserializers(sft, featureEncoding)
  val indexValueEncoder = IndexValueEncoder(sft, version)

  /**
   * Plan the query, but don't execute it - used for explain query
   */
  def planQuery(query: Query, output: ExplainerOutputType = log): Seq[QueryPlan] =
    getStrategyPlans(query, output)._1.map(_.plan)

  /**
   * Execute a query against geomesa
   */
  def query(query: Query): SFIter = {
    val (plans, numClauses) = getStrategyPlans(query, log)
    val dedupe = numClauses > 1 || plans.exists(_.plan.hasDuplicates)
    executePlans(query, plans, dedupe)
  }

  /**
   * Execute a query plan. Split out to allow for easier testing.
   */
  def executePlans(query: Query, strategyPlans: Seq[StrategyPlan], deduplicate: Boolean): SFIter = {
    val features = strategyPlans.iterator.ciFlatMap { sp =>
      sp.strategy.execute(sp.plan, acc, log).map(sp.plan.kvsToFeatures)
    }

    val dedupedFeatures = if (deduplicate) new DeDuplicatingIterator(features) else features

    val sortedFeatures = if (query.getSortBy != null && query.getSortBy.length > 0) {
      new LazySortedIterator(dedupedFeatures, query.getSortBy)
    } else {
      dedupedFeatures
    }

    adaptIterator(query, sortedFeatures)
  }

  /**
   * Set up the query plans and strategies used to execute them.
   * Returns the strategy plans and the number of distinct OR clauses, needed for determining deduplication
   */
  private def getStrategyPlans(query: Query, output: ExplainerOutputType): (Seq[StrategyPlan], Int) = {
    output(s"Planning ${ExplainerOutputType.toString(query)}")
    implicit val timings = new TimingsImpl
    val (queryPlans, numClauses) = profile({
      val isDensity = query.getHints.containsKey(BBOX_KEY)
      if (isDensity) {
        val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
        val q1 = new Query(sft.getTypeName, ff.bbox(ff.property(sft.getGeometryDescriptor.getLocalName), env))
        val mixedQuery = DataUtilities.mixQueries(q1, query, "geomesa.mixed.query")
        val strategy = QueryStrategyDecider.chooseStrategy(sft, mixedQuery, hints, version)
        val plans = strategy.getQueryPlans(mixedQuery, this, output)
        (plans.map { p => StrategyPlan(strategy, p) }, 1)
      } else {
        // As a pre-processing step, we examine the query/filter and split it into multiple queries.
        // TODO Work to make the queries non-overlapping
        val splitQueries = splitQueryOnOrs(query, output)
        val plans = splitQueries.flatMap { q =>
          val strategy = QueryStrategyDecider.chooseStrategy(sft, q, hints, version)
          output(s"Strategy: ${strategy.getClass.getCanonicalName}")
          output(s"Transforms: ${getTransformDefinition(query).getOrElse("None")}")
          strategy.getQueryPlans(q, this, output).map { plan =>
            output(s"Table: ${plan.table}")
            output(s"Column Families${if (plan.columnFamilies.isEmpty) ": all" else s" (${plan.columnFamilies.size}): ${plan.columnFamilies.take(20)}"} ")
            output(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).mkString(", ")}")
            output(s"Iterators (${plan.iterators.size}): ${plan.iterators.mkString("[", ", ", "]")}")
            StrategyPlan(strategy, plan)
          }
        }
        (plans, splitQueries.length)
      }
    }, "plan")
    output(s"Query planning took ${timings.time("plan")} ms for $numClauses distinct queries.")
    (queryPlans, numClauses)
  }

  // This function decodes/transforms that Iterator of Accumulo Key-Values into an Iterator of SimpleFeatures
  def defaultKVsToFeatures(query: Query): FeatureFunction = {
    // Perform a projecting decode of the simple feature
    val returnSFT = getReturnSFT(query)
    val deserializer = SimpleFeatureDeserializers(returnSFT, featureEncoding)
    (kv: Entry[Key, Value]) => {
      val sf = deserializer.deserialize(kv.getValue.get)
      applyVisibility(sf, kv.getKey)
      sf
    }
  }

  def adaptIterator(query: Query, features: SFIter): SFIter = {
    // Decode according to the SFT return type.
    // if this is a density query, expand the map
    if (query.getHints.containsKey(DENSITY_KEY)) {
      adaptDensityIterator(features)
    } else if (query.getHints.containsKey(TEMPORAL_DENSITY_KEY)) {
      adaptTemporalIterator(features, getReturnSFT(query), query.getHints.containsKey(RETURN_ENCODED))
    } else if (query.getHints.containsKey(MAP_AGGREGATION_KEY)) {
      adaptMapAggregationIterator(features, getReturnSFT(query), query)
    } else {
      features
    }
  }

  def adaptDensityIterator(features: SFIter): SFIter = features.flatMap(DensityIterator.expandFeature)

  def adaptTemporalIterator(features: SFIter, sft: SimpleFeatureType, returnEncoded: Boolean): SFIter = {
    val timeSeriesStrings = features.map(f => decodeTimeSeries(f.getAttribute(TIME_SERIES).toString))
    val summedTimeSeries = timeSeriesStrings.reduceOption(combineTimeSeries)

    val feature = summedTimeSeries.map { sum =>
      val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(sft)
      if (returnEncoded) {
        featureBuilder.add(TemporalDensityIterator.encodeTimeSeries(sum))
      } else {
        featureBuilder.add(timeSeriesToJSON(sum))
      }
      featureBuilder.add(GeometryUtils.zeroPoint) // Filler value as Feature requires a geometry
      featureBuilder.buildFeature(null)
    }

    feature.iterator
  }

  def adaptMapAggregationIterator(features: SFIter, sft: SimpleFeatureType, query: Query): SFIter = {
    val aggregateKeyName = query.getHints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]

    val maps = features.map(_.getAttribute(aggregateKeyName).asInstanceOf[JMap[AnyRef, Int]].asScala)

    if (maps.nonEmpty) {
      val reducedMap = sumNumericValueMutableMaps(maps.toIterable).toMap // to immutable map

      val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(sft)
      featureBuilder.reset()
      featureBuilder.add(reducedMap)
      featureBuilder.add(GeometryUtils.zeroPoint) // Filler value as Feature requires a geometry
      val result = featureBuilder.buildFeature(null)

      Iterator(result)
    } else {
      CloseableIterator.empty
    }
  }

  // This function calculates the SimpleFeatureType of the returned SFs.
  private def getReturnSFT(query: Query): SimpleFeatureType =
    if (query.getHints.containsKey(DENSITY_KEY)) {
      SimpleFeatureTypes.createType(sft.getTypeName, DensityIterator.DENSITY_FEATURE_SFT_STRING)
    } else if (query.getHints.containsKey(TEMPORAL_DENSITY_KEY)) {
      createFeatureType(sft)
    } else if (query.getHints.containsKey(MAP_AGGREGATION_KEY)) {
      val mapAggregationAttribute = query.getHints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]
      val spec = MapAggregatingIterator.projectedSFTDef(mapAggregationAttribute, sft)
      SimpleFeatureTypes.createType(sft.getTypeName, spec)
    } else {
      getTransformSchema(query).getOrElse(sft)
    }
}

object QueryPlanner {

  val iteratorPriority_RowRegex                        = 0
  val iteratorPriority_AttributeIndexFilteringIterator = 10
  val iteratorPriority_AttributeIndexIterator          = 200
  val iteratorPriority_AttributeUniqueIterator         = 300
  val iteratorPriority_ColFRegex                       = 100
  val iteratorPriority_SpatioTemporalIterator          = 200
  val iteratorPriority_SimpleFeatureFilteringIterator  = 300
  val iteratorPriority_AnalysisIterator                = 400

  type KVIter = CloseableIterator[Entry[Key,Value]]
  type SFIter = CloseableIterator[SimpleFeature]

  def splitQueryOnOrs(query: Query, output: ExplainerOutputType): Seq[Query] = {
    val originalFilter = query.getFilter
    output(s"Original filter: $originalFilter")

    val rewrittenFilter = rewriteFilterInDNF(originalFilter)
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

  def applyVisibility(sf: SimpleFeature, key: Key): Unit = {
    val visibility = key.getColumnVisibility
    if (!EMPTY_VIZ.equals(visibility)) {
      SecurityUtils.setFeatureVisibility(sf, visibility.toString)
    }
  }
}

class LazySortedIterator(features: CloseableIterator[SimpleFeature],
                         sortBy: Array[SortBy]) extends CloseableIterator[SimpleFeature] {

  private lazy val sorted: CloseableIterator[SimpleFeature] = {

    val sortOrdering = sortBy.map {
      case SortBy.NATURAL_ORDER => Ordering.by[SimpleFeature, String](_.getID)
      case SortBy.REVERSE_ORDER => Ordering.by[SimpleFeature, String](_.getID).reverse
      case sb                   =>
        val prop = sb.getPropertyName.getPropertyName
        val ord  = attributeToComparable(prop)
        if (sb.getSortOrder == SortOrder.DESCENDING) ord.reverse else ord
    }
    val comp: (SimpleFeature, SimpleFeature) => Boolean =
      if (sortOrdering.length == 1) {
        // optimized case for one ordering
        val ret = sortOrdering.head
        (l, r) => ret.compare(l, r) < 0
      }  else {
        (l, r) => sortOrdering.map(_.compare(l, r)).find(_ != 0).getOrElse(0) < 0
      }
    // use ListBuffer for constant append time and size
    val buf = scala.collection.mutable.ListBuffer.empty[SimpleFeature]
    while (features.hasNext) {
      buf.append(features.next())
    }
    features.close()
    CloseableIterator(buf.sortWith(comp).iterator)
  }

  def attributeToComparable[T <: Comparable[T]](prop: String)(implicit ct: ClassTag[T]): Ordering[SimpleFeature] =
    Ordering.by[SimpleFeature, T](_.getAttribute(prop).asInstanceOf[T])(new Ordering[T] {
      val evo = implicitly[Ordering[T]]

      override def compare(x: T, y: T): Int = {
        (x, y) match {
          case (null, null) => 0
          case (null, _)    => -1
          case (_, null)    => 1
          case (_, _)       => evo.compare(x, y)
        }
      }
    })

  override def hasNext: Boolean = sorted.hasNext

  override def next(): SimpleFeature = sorted.next()

  override def close(): Unit = {}
}

