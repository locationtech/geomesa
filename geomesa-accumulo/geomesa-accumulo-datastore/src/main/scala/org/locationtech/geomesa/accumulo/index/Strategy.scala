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

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting, Scanner}
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Interval
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner._
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators.{FEATURE_ENCODING, _}
import org.locationtech.geomesa.accumulo.util.{BatchMultiScanner, CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.Random

trait Strategy extends Logging {

  /**
   * Plans the query - strategy implementations need to define this
   */
  def getQueryPlans(query: Query, queryPlanner: QueryPlanner, output: ExplainerOutputType): Seq[QueryPlan]

  /**
   * Execute a query against this strategy
   */
  def execute(plan: QueryPlan, acc: AccumuloConnectorCreator, output: ExplainerOutputType): KVIter = {
    try {
      SelfClosingIterator(getScanner(plan, acc))
    } catch {
      case e: Exception =>
        logger.error(s"Error in creating scanner: $e", e)
        // since GeoTools would eat the error and return no records anyway,
        // there's no harm in returning an empty iterator.
        Iterator.empty
    }
  }

  /**
   * Creates a scanner based on a query plan
   */
  private def getScanner(queryPlan: QueryPlan, acc: AccumuloConnectorCreator): KVIter =
    queryPlan match {
      case qp: ScanPlan =>
        val scanner = acc.getScanner(qp.table)
        configureScanner(scanner, qp)
        SelfClosingIterator(scanner)
      case qp: BatchScanPlan =>
        if (qp.ranges.isEmpty) {
          logger.warn("Query plan resulted in no valid ranges - nothing will be returned.")
          CloseableIterator(Iterator.empty)
        } else {
          val batchScanner = acc.getBatchScanner(qp.table, qp.numThreads)
          configureBatchScanner(batchScanner, qp)
          SelfClosingIterator(batchScanner)
        }
      case qp: JoinPlan =>
        val primary = if (qp.ranges.length == 1) {
          val scanner = acc.getScanner(qp.table)
          configureScanner(scanner, qp)
          scanner
        } else {
          val batchScanner = acc.getBatchScanner(qp.table, qp.numThreads)
          configureBatchScanner(batchScanner, qp)
          batchScanner
        }
        val jqp = qp.joinQuery
        val secondary = acc.getBatchScanner(jqp.table, jqp.numThreads)
        configureBatchScanner(secondary, jqp)

        val bms = new BatchMultiScanner(primary, secondary, qp.joinFunction)
        SelfClosingIterator(bms.iterator, () => bms.close())
    }
}


object Strategy {

  def configureBatchScanner(bs: BatchScanner, qp: QueryPlan) {
    qp.iterators.foreach { i => bs.addScanIterator(i) }
    bs.setRanges(qp.ranges)
    qp.columnFamilies.foreach { c => bs.fetchColumnFamily(c) }
  }

  def configureScanner(scanner: Scanner, qp: QueryPlan) {
    qp.iterators.foreach { i => scanner.addScanIterator(i) }
    qp.ranges.headOption.foreach(scanner.setRange)
    qp.columnFamilies.foreach { c => scanner.fetchColumnFamily(c) }
  }

  def configureFeatureEncoding(cfg: IteratorSetting, featureEncoding: SerializationType) {
    cfg.addOption(FEATURE_ENCODING, featureEncoding.toString)
  }

  def configureStFilter(cfg: IteratorSetting, filter: Option[Filter]) = {
    filter.foreach { f => cfg.addOption(ST_FILTER_PROPERTY_NAME, ECQL.toCQL(f)) }
  }

  def configureVersion(cfg: IteratorSetting, version: Int) =
    cfg.addOption(GEOMESA_ITERATORS_VERSION, version.toString)

  def configureFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  def configureFeatureTypeName(cfg: IteratorSetting, featureType: String) =
    cfg.addOption(GEOMESA_ITERATORS_SFT_NAME, featureType)

  def configureIndexValues(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SFT_INDEX_VALUE, encodedSimpleFeatureType)
  }

  def configureEcqlFilter(cfg: IteratorSetting, ecql: Option[String]) =
    ecql.foreach(filter => cfg.addOption(GEOMESA_ITERATORS_ECQL_FILTER, filter))

  // store transform information into an Iterator's settings
  def configureTransforms(cfg: IteratorSetting, query:Query) =
    for {
      transformOpt  <- org.locationtech.geomesa.accumulo.index.getTransformDefinition(query)
      transform     = transformOpt.asInstanceOf[String]
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM, transform)
      sfType        <- org.locationtech.geomesa.accumulo.index.getTransformSchema(query)
      encodedSFType = SimpleFeatureTypes.encodeType(sfType)
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM_SCHEMA, encodedSFType)
    } yield Unit

  def configureRecordTableIterator(
      simpleFeatureType: SimpleFeatureType,
      featureEncoding: SerializationType,
      ecql: Option[Filter],
      query: Query): IteratorSetting = {

    val cfg = new IteratorSetting(
      iteratorPriority_SimpleFeatureFilteringIterator,
      classOf[RecordTableIterator].getSimpleName,
      classOf[RecordTableIterator]
    )
    configureFeatureType(cfg, simpleFeatureType)
    configureFeatureEncoding(cfg, featureEncoding)
    configureEcqlFilter(cfg, ecql.map(ECQL.toCQL))
    configureTransforms(cfg, query)
    cfg
  }

  def randomPrintableString(length:Int=5) : String = (1 to length).
    map(i => Random.nextPrintableChar()).mkString

  def getDensityIterCfg(query: Query,
                        geometryToCover: Geometry,
                        schema: String,
                        featureEncoding: SerializationType,
                        featureType: SimpleFeatureType) = query match {
    case _ if query.getHints.containsKey(DENSITY_KEY) =>
      val clazz = classOf[DensityIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val width = query.getHints.get(WIDTH_KEY).asInstanceOf[Int]
      val height = query.getHints.get(HEIGHT_KEY).asInstanceOf[Int]
      val polygon = if (geometryToCover == null) null else geometryToCover.getEnvelope.asInstanceOf[Polygon]

      DensityIterator.configure(cfg, polygon, width, height)

      cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    case _ if query.getHints.containsKey(TEMPORAL_DENSITY_KEY) =>
      val clazz = classOf[TemporalDensityIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val interval = query.getHints.get(TIME_INTERVAL_KEY).asInstanceOf[Interval]
      val buckets = query.getHints.get(TIME_BUCKETS_KEY).asInstanceOf[Int]

      TemporalDensityIterator.configure(cfg, interval, buckets)

      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    case _ if query.getHints.containsKey(MAP_AGGREGATION_KEY) =>
      val clazz = classOf[MapAggregatingIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val mapAttribute = query.getHints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]

      MapAggregatingIterator.configure(cfg, mapAttribute)

      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    case _ => None
  }
}

trait StrategyProvider {

  /**
   * Returns details on a potential strategy if the filter is valid for this strategy.
   *
   * @param filter
   * @param sft
   * @return
   */
  def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints): Option[StrategyDecision]
}

case class StrategyDecision(strategy: Strategy, cost: Long)
case class StrategyPlan(strategy: Strategy, plan: QueryPlan)