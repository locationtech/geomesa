/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting, Scanner}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Interval
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner._
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
   * The filter this strategy will execute
   */
  def filter: QueryFilter

  /**
   * Plans the query - strategy implementations need to define this
   */
  def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType): QueryPlan
}


object Strategy extends Logging {

  // enumeration of the various strategies we implement - don't forget to add new impls here
  object StrategyType extends Enumeration {
    type StrategyType = Value
    val Z3, ST, RECORD, ATTRIBUTE = Value
  }

  /**
   * Execute a query against this strategy
   */
  def execute(plan: QueryPlan, acc: AccumuloConnectorCreator): KVIter = {
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
          QueryProperties.SCAN_BATCH_RANGES.option.map(_.toInt) match {
            case Some(size) if size < qp.ranges.length =>
              // break up the ranges into groups that are manageable in memory
              val groups = qp.ranges.grouped(size)
              SelfClosingIterator(groups).ciFlatMap { group =>
                val batchScanner = acc.getBatchScanner(qp.table, qp.numThreads)
                configureBatchScanner(batchScanner, qp.copy(ranges = group))
                SelfClosingIterator(batchScanner)
              }

            case _ =>
              val batchScanner = acc.getBatchScanner(qp.table, qp.numThreads)
              configureBatchScanner(batchScanner, qp)
              SelfClosingIterator(batchScanner)
          }
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
  def configureTransforms(cfg: IteratorSetting, hints: Hints) =
    for {
      transformOpt  <- hints.getTransformDefinition
      transform     = transformOpt.asInstanceOf[String]
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM, transform)
      sfType        <- hints.getTransformSchema
      encodedSFType = SimpleFeatureTypes.encodeType(sfType)
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM_SCHEMA, encodedSFType)
    } yield Unit

  def configureRecordTableIterator(
      simpleFeatureType: SimpleFeatureType,
      featureEncoding: SerializationType,
      ecql: Option[Filter],
      hints: Hints): IteratorSetting = {

    val cfg = new IteratorSetting(
      iteratorPriority_SimpleFeatureFilteringIterator,
      classOf[RecordTableIterator].getSimpleName,
      classOf[RecordTableIterator]
    )
    configureFeatureType(cfg, simpleFeatureType)
    configureFeatureEncoding(cfg, featureEncoding)
    configureEcqlFilter(cfg, ecql.map(ECQL.toCQL))
    configureTransforms(cfg, hints)
    cfg
  }

  def randomPrintableString(length:Int=5) : String = (1 to length).
    map(i => Random.nextPrintableChar()).mkString

  def configureAggregatingIterator(hints: Hints,
                                   geometryToCover: Geometry,
                                   schema: String,
                                   featureEncoding: SerializationType,
                                   featureType: SimpleFeatureType) = hints match {
    case _ if hints.containsKey(TEMPORAL_DENSITY_KEY) =>
      val clazz = classOf[TemporalDensityIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val interval = hints.get(TIME_INTERVAL_KEY).asInstanceOf[Interval]
      val buckets = hints.get(TIME_BUCKETS_KEY).asInstanceOf[Int]

      TemporalDensityIterator.configure(cfg, interval, buckets)

      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    case _ if hints.containsKey(MAP_AGGREGATION_KEY) =>
      val clazz = classOf[MapAggregatingIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val mapAttribute = hints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]

      MapAggregatingIterator.configure(cfg, mapAttribute)

      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    case _ => None
  }
}

trait StrategyProvider {

  /**
   * Gets the estimated cost of running the query
   */
  def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints): Int
}
