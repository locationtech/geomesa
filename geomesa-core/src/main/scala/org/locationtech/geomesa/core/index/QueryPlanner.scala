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
import java.util.{Map => JMap}

import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.{DataUtilities, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.iterators.TemporalDensityIterator._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.sumNumericValueMutableMaps
import org.locationtech.geomesa.core.util.CloseableIterator
import org.locationtech.geomesa.core.util.CloseableIterator._
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.{ScalaSimpleFeatureFactory, SimpleFeatureDecoder, SimpleFeatureEncoder}
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
                        featureEncoding: FeatureEncoding,
                        stSchema: String,
                        acc: AccumuloConnectorCreator,
                        hints: StrategyHints,
                        version: Int) extends ExplainingLogging with IndexFilterHelpers with MethodProfiling {

  import org.locationtech.geomesa.core.index.QueryPlanner._

  case class StrategyPlan(strategy: Strategy, plan: QueryPlan)

  val featureEncoder = SimpleFeatureEncoder(sft, featureEncoding)
  val featureDecoder = SimpleFeatureDecoder(sft, featureEncoding)
  val indexValueEncoder = IndexValueEncoder(sft, version)

  /**
   * Execute a query against geomesa
   */
  def query(query: Query): SFIter = {
    // set up the iterator based on the query
    val accumuloIterator = getIterator(query, log)
    // convert accumulo results to SimpleFeatures
    adaptIterator(accumuloIterator, query)
  }

  /**
   * Plan the query, but don't execute it - used for explain query
   */
  def planQuery(query: Query, output: ExplainerOutputType = log): Seq[QueryPlan] =
    getQueryPlans(query, output).map(_.plan)

  /**
   * Gets the accumulo iterator returning raw key/value pairs
   */
  private def getIterator(query: Query, output: ExplainerOutputType): KVIter = {
    val queryPlans = getQueryPlans(query, output)

    val hasDupes = queryPlans.length > 1 || queryPlans.exists(_.plan.hasDuplicates)

    val rawKvs = queryPlans.iterator.ciFlatMap(sp => sp.strategy.execute(sp.plan, acc, output))

    if (hasDupes) {
      val dedupe = (key: Key, value: Value) => featureDecoder.extractFeatureId(value.get)
      new DeDuplicatingIterator(rawKvs, dedupe)
    } else {
      rawKvs
    }
  }

  /**
   * Set up the query plans and strategies used to execute them
   */
  private def getQueryPlans(query: Query, output: ExplainerOutputType): Seq[StrategyPlan] = {
    output(s"Planning ${ExplainerOutputType.toString(query)}")
    implicit val timings = new TimingsImpl
    val queryPlans = profile({
      val isDensity = query.getHints.containsKey(BBOX_KEY)
      if (isDensity) {
        val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
        val q1 = new Query(sft.getTypeName, ff.bbox(ff.property(sft.getGeometryDescriptor.getLocalName), env))
        val mixedQuery = DataUtilities.mixQueries(q1, query, "geomesa.mixed.query")
        val strategy = QueryStrategyDecider.chooseStrategy(sft, mixedQuery, hints, version)
        val plan = strategy.getQueryPlan(mixedQuery, this, output)
        Seq(StrategyPlan(strategy, plan))
      } else {
        // As a pre-processing step, we examine the query/filter and split it into multiple queries.
        // TODO Work to make the queries non-overlapping
        splitQueryOnOrs(query, output).map { q =>
          val strategy = QueryStrategyDecider.chooseStrategy(sft, q, hints, version)
          output(s"Strategy: ${strategy.getClass.getCanonicalName}")
          output(s"Transforms: ${getTransformDefinition(query).getOrElse("None")}")
          val plan = strategy.getQueryPlan(q, this, output)
          output(s"Table: ${plan.table}")
          output(s"Column Families${if (plan.columnFamilies.isEmpty) ": all" else s" (${plan.columnFamilies.size}): ${plan.columnFamilies.take(20)}"} ")
          output(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).mkString(", ")}")
          output(s"Iterators (${plan.iterators.size}): ${plan.iterators.mkString("[", ", ", "]")}")
          StrategyPlan(strategy, plan)
        }
      }
    }, "plan")
    output(s"Query Planning took ${timings.time("plan")} milliseconds.")
    queryPlans
  }

  // This function decodes/transforms that Iterator of Accumulo Key-Values into an Iterator of SimpleFeatures.
  def adaptIterator(accumuloIterator: KVIter, query: Query): SFIter = {
    // Perform a projecting decode of the simple feature
    val returnSFT = getReturnSFT(query)
    val decoder = SimpleFeatureDecoder(returnSFT, featureEncoding)

    // Decode according to the SFT return type.
    // if this is a density query, expand the map
    if (query.getHints.containsKey(DENSITY_KEY)) {
      adaptDensityIterator(accumuloIterator, decoder)
    } else if (query.getHints.containsKey(TEMPORAL_DENSITY_KEY)) {
      adaptTemporalIterator(accumuloIterator, returnSFT, decoder, query.getHints.containsKey(RETURN_ENCODED))
    } else if (query.getHints.containsKey(MAP_AGGREGATION_KEY)) {
      adaptMapAggregationIterator(accumuloIterator, query, returnSFT, decoder)
    } else if (query.getHints.containsKey(QUERY_SIZE_KEY)) {
      adaptQuerySizeIterator(accumuloIterator, query, decoder)
    } else {
      adaptStandardIterator(accumuloIterator, query, decoder)
    }
  }

  /**
   * Standard iterator of simple features
   */
  def adaptStandardIterator(accumuloIterator: KVIter, query: Query, decoder: SimpleFeatureDecoder): SFIter = {
    val features = accumuloIterator.map { kv =>
      val sf = decoder.decode(kv.getValue.get)
      val visibility = kv.getKey.getColumnVisibility
      if (!EMPTY_VIZ.equals(visibility)) {
        SecurityUtils.setFeatureVisibility(sf, visibility.toString)
      }
      sf
    }
    if (query.getSortBy != null && !query.getSortBy.isEmpty) {
      new LazySortedIterator(features, query.getSortBy)
    } else {
      features
    }
  }

  /**
   * Standard iterator of simple features
   */
  def adaptQuerySizeIterator(accumuloIterator: KVIter, query: Query, decoder: SimpleFeatureDecoder): SFIter = {

    val querySizeIterator = accumuloIterator.map { kv =>
      decoder.decode(kv.getValue.get)
    }

    var scannedBytes: Long = 0
    var scannedRecords: Long = 0
    var resultBytes: Long = 0
    var resultRecords: Long = 0
    while(querySizeIterator.hasNext) { // TODO How do I do this in a Scala-y way?
      val cur = querySizeIterator.next()
      scannedBytes += cur.getAttribute(QuerySizeIterator.SCAN_BYTES_ATTRIBUTE).asInstanceOf[Long]
      scannedRecords += cur.getAttribute(QuerySizeIterator.SCAN_RECORDS_ATTRIBUTE).asInstanceOf[Long]
      resultBytes += cur.getAttribute(QuerySizeIterator.RESULT_BYTES_ATTRIBUTE).asInstanceOf[Long]
      resultRecords += cur.getAttribute(QuerySizeIterator.RESULT_RECORDS_ATTRIBUTE).asInstanceOf[Long]
    }
    val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(getReturnSFT(query))
    featureBuilder.set(QuerySizeIterator.SCAN_BYTES_ATTRIBUTE, scannedBytes)
    featureBuilder.set(QuerySizeIterator.SCAN_RECORDS_ATTRIBUTE, scannedRecords)
    featureBuilder.set(QuerySizeIterator.RESULT_BYTES_ATTRIBUTE, resultBytes)
    featureBuilder.set(QuerySizeIterator.RESULT_RECORDS_ATTRIBUTE, resultRecords)

    CloseableIterator(Iterator(featureBuilder.buildFeature("resultFeature")))
  }

  def adaptTemporalIterator(accumuloIterator: KVIter,
                            returnSFT: SimpleFeatureType,
                            decoder: SimpleFeatureDecoder,
                            returnEncoded: Boolean): SFIter = {
    val timeSeriesStrings = accumuloIterator.map { kv =>
      val encoded = decoder.decode(kv.getValue.get).getAttribute(TIME_SERIES).toString
      decodeTimeSeries(encoded)
    }
    val summedTimeSeries = timeSeriesStrings.reduceOption(combineTimeSeries)

    val feature = summedTimeSeries.map { sum =>
      val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(returnSFT)
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

  def adaptDensityIterator(accumuloIterator: KVIter, decoder: SimpleFeatureDecoder): SFIter =
    accumuloIterator.flatMap(kv => DensityIterator.expandFeature(decoder.decode(kv.getValue.get)))

  def adaptMapAggregationIterator(accumuloIterator: CloseableIterator[Entry[Key, Value]],
                                  query: Query,
                                  returnSFT: SimpleFeatureType,
                                  decoder: SimpleFeatureDecoder): CloseableIterator[SimpleFeature] = {
    val aggregateKeyName = query.getHints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]

    val maps = accumuloIterator.map { kv =>
      decoder.decode(kv.getValue.get).getAttribute(aggregateKeyName).asInstanceOf[JMap[AnyRef, Int]].asScala
    }

    if (maps.nonEmpty) {
      val reducedMap = sumNumericValueMutableMaps(maps.toIterable).toMap // to immutable map

      val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(returnSFT)
      featureBuilder.reset()
      featureBuilder.add(reducedMap)
      featureBuilder.add(GeometryUtils.zeroPoint) // Filler value as Feature requires a geometry
      val result = featureBuilder.buildFeature(null)

      Iterator(result)
    } else Iterator.empty
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
    } else if (query.getHints.containsKey(QUERY_SIZE_KEY)) {
      val spec = QuerySizeIterator.QUERY_SIZE_FEATURE_SFT_STRING
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

