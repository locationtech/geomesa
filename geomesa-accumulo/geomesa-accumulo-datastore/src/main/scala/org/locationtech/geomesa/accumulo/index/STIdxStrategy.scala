/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.GEOMESA_ITERATORS_IS_DENSITY_TYPE
import org.locationtech.geomesa.accumulo.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner._
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.features.SerializationType
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

@deprecated("z2")
class STIdxStrategy(val filter: QueryFilter) extends Strategy with LazyLogging with IndexFilterHelpers {

  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val acc             = queryPlanner.ds
    val sft             = queryPlanner.sft
    val version         = sft.getSchemaVersion
    val schema          = queryPlanner.ds.getIndexSchemaFmt(sft)
    val featureEncoding = queryPlanner.ds.getFeatureEncoding(sft)
    val keyPlanner      = IndexSchema.buildKeyPlanner(schema)
    val cfPlanner       = IndexSchema.buildColumnFamilyPlanner(schema)

    output(s"Scanning ST index table for feature type ${sft.getTypeName}")
    output(s"Filter: ${filter.primary} ${filter.secondary.map(_.toString).getOrElse("")}")

    val dtgField = sft.getDtgField

    val (geomFilters, temporalFilters) = filter.primary.filter(_ != Filter.INCLUDE).partition(isSpatialFilter)
    val ecql = filter.secondary

    output(s"Geometry filters: ${filtersToString(geomFilters)}")
    output(s"Temporal filters: ${filtersToString(temporalFilters)}")

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = geomFilters.flatMap(decomposeToGeometry)

    output(s"GeomsToCover: $geomsToCover")

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    val interval = extractInterval(temporalFilters, dtgField)
    val geometryToCover = netGeom(collectionToCover)

    val keyPlanningFilter = buildFilter(geometryToCover, interval)
    // This catches the case when a whole world query slips through DNF/CNF
    // The union on this geometry collection is necessary at the moment but is not true
    // If given spatial predicates like disjoint.
    val ofilter = if (isWholeWorld(geometryToCover)) {
      filterListAsAnd(temporalFilters)
    } else filterListAsAnd(geomFilters ++ temporalFilters)

    if (ofilter.isEmpty) {
      logger.warn(s"Querying Accumulo without SpatioTemporal filter.")
    }

    val oint  = IndexSchema.somewhen(interval)

    output(s"STII Filter: ${ofilter.getOrElse("No STII Filter")}")
    output(s"Interval:  ${oint.getOrElse("No interval")}")
    output(s"Filter: ${Option(keyPlanningFilter).getOrElse("No Filter")}")

    val (iterators, kvsToFeatures, useIndexEntries, hasDupes) = if (hints.isDensityQuery) {
      val (width, height) = hints.getDensityBounds.get
      val envelope = hints.getDensityEnvelope.get
      val weight = hints.getDensityWeight
      val p = iteratorPriority_AnalysisIterator
      val iter =
        DensityIterator.configure(sft, featureEncoding, schema, filter.filter, envelope, width, height, weight, p)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), false, false)
    } else if (featureEncoding == SerializationType.KRYO &&
        // we have some special handling for bin line dates not implemented in the bin iter yet
        !(sft.isLines && hints.isBinQuery)) {
      // TODO GEOMESA-822 add bin line dates to distributed bin aggregation
      if (hints.isBinQuery) {
        // use the server side aggregation
        val iter = BinAggregatingIterator.configureDynamic(sft, filter.filter, hints, sft.nonPoints)
        (Seq(iter), BinAggregatingIterator.kvsToFeatures(), false, false)
      } else if (hints.isStatsIteratorQuery) {
        val iter = KryoLazyStatsIterator.configure(sft, filter.filter, hints, sft.nonPoints)
        (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), false, false)
      } else {
        val iters = KryoLazyFilterTransformIterator.configure(sft, filter.filter, hints).toSeq
        (iters, queryPlanner.defaultKVsToFeatures(hints), false, sft.nonPoints)
      }
    } else {
      // legacy iterators
      val iteratorConfig = IteratorTrigger.chooseIterator(filter.filter.getOrElse(Filter.INCLUDE), ecql, hints, sft)
      val stiiIterCfg = getSTIIIterCfg(iteratorConfig, hints, sft, ofilter, ecql, featureEncoding, version)
      val aggIterCfg = configureAggregatingIterator(hints, geometryToCover, schema, featureEncoding, sft)

      val indexEntries = iteratorConfig.iterator match {
        case IndexOnlyIterator      => true
        case SpatioTemporalIterator => false
      }
      val iters = Seq(stiiIterCfg) ++ aggIterCfg
      val kvs = if (hints.isBinQuery) {
        BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, hints, featureEncoding)
      } else {
        queryPlanner.defaultKVsToFeatures(hints)
      }
      (iters, kvs, indexEntries, sft.nonPoints)
    }

    // set up row ranges and regular expression filter
    val qp = planQuery(keyPlanningFilter, useIndexEntries, output, keyPlanner, cfPlanner)

    val table = acc.getTableName(sft.getTypeName, SpatioTemporalTable)
    val numThreads = acc.getSuggestedThreads(sft.getTypeName, SpatioTemporalTable)
    qp.copy(table = table, iterators = iterators, kvsToFeatures = kvsToFeatures,
      numThreads = numThreads, hasDuplicates = hasDupes)
  }

  private def getSTIIIterCfg(iteratorConfig: IteratorConfig,
                     hints: Hints,
                     featureType: SimpleFeatureType,
                     stFilter: Option[Filter],
                     ecqlFilter: Option[Filter],
                     featureEncoding: SerializationType,
                     version: Int): IteratorSetting = {
    iteratorConfig.iterator match {
      case IndexOnlyIterator =>
        configureIndexIterator(featureType, hints, featureEncoding, stFilter,
          iteratorConfig.transformCoversFilter, version)
      case SpatioTemporalIterator =>
        configureSpatioTemporalIntersectingIterator(featureType, hints, featureEncoding, stFilter,
          ecqlFilter, hints.isDensityQuery)
    }
  }

  // establishes the regular expression that defines (minimally) acceptable rows
  def configureRowRegexIterator(regex: String): IteratorSetting = {
    val name = "regexRow-" + randomPrintableString(5)
    val cfg = new IteratorSetting(iteratorPriority_RowRegex, name, classOf[RegExFilter])
    RegExFilter.setRegexs(cfg, regex, null, null, null, false)
    cfg
  }

  // returns an iterator over [key,value] pairs where the key is taken from the index row and the value is a SimpleFeature,
  // which is either read directory from the data row  value or generated from the encoded index row value
  // -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureIndexIterator(
      featureType: SimpleFeatureType,
      hints: Hints,
      featureEncoding: SerializationType,
      filter: Option[Filter],
      transformsCoverFilter: Boolean,
      version: Int): IteratorSetting = {

    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),classOf[IndexIterator])

    configureStFilter(cfg, filter)

    configureVersion(cfg, version)
    if (transformsCoverFilter) {
      // apply the transform directly to the index iterator
      hints.getTransformSchema.foreach(testType => configureFeatureType(cfg, testType))
    } else {
      // we need to evaluate the original feature before transforming
      // transforms are applied afterwards
      configureFeatureType(cfg, featureType)
      configureTransforms(cfg, hints)
    }
    configureIndexValues(cfg, featureType)
    configureFeatureEncoding(cfg, featureEncoding)
    cfg
  }

  // returns only the data entries -- no index entries -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureSpatioTemporalIntersectingIterator(
      featureType: SimpleFeatureType,
      hints: Hints,
      featureEncoding: SerializationType,
      stFilter: Option[Filter],
      ecqlFilter: Option[Filter],
      isDensity: Boolean): IteratorSetting = {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),
      classOf[SpatioTemporalIntersectingIterator])
    val combinedFilter = (stFilter, ecqlFilter) match {
      case (Some(st), Some(ecql)) => filterListAsAnd(Seq(st, ecql))
      case (Some(_), None)        => stFilter
      case (None, Some(_))        => ecqlFilter
      case (None, None)           => None
    }
    configureFeatureType(cfg, featureType)
    configureFeatureEncoding(cfg, featureEncoding)
    configureTransforms(cfg, hints)
    configureEcqlFilter(cfg, combinedFilter.map(ECQL.toCQL))
    if (isDensity) {
      cfg.addOption(GEOMESA_ITERATORS_IS_DENSITY_TYPE, "isDensity")
    }
    cfg
  }

  def planQuery(filter: KeyPlanningFilter,
                useIndexEntries: Boolean,
                output: ExplainerOutputType,
                keyPlanner: KeyPlanner,
                cfPlanner: ColumnFamilyPlanner): BatchScanPlan = {
    output(s"Planning query")

    val keyPlan = keyPlanner.getKeyPlan(filter, useIndexEntries, output)

    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new org.apache.accumulo.core.data.Range(r.start, r.end))
      case _ => Seq(new org.apache.accumulo.core.data.Range())
    }

    // if you have a list of distinct column-family entries, fetch them
    val cf = columnFamilies match {
      case KeyList(keys) => keys.map { cf => new Text(cf) }
      case _             => Seq()
    }

    // partially fill in, rest will be filled in later
    BatchScanPlan(null, accRanges, null, cf, null, -1, hasDuplicates = false)
  }
}

@deprecated("z2")
object STIdxStrategy extends StrategyProvider {

  /**
   * Gets the estimated cost of running the query. Currently, cost is hard-coded to sort between
   * strategies the way we want. STIdx should be more than id lookups (at 1), high-cardinality attributes
   * (at 1) and z3 queries (at 200) but less than unknown cardinality attributes (at 999).
   *
   * Eventually cost will be computed based on dynamic metadata and the query.
   */
  override def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints) = 400
}
