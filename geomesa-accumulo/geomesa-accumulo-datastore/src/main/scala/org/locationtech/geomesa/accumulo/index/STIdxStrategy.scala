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
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.GEOMESA_ITERATORS_IS_DENSITY_TYPE
import org.locationtech.geomesa.accumulo.filter._
import org.locationtech.geomesa.accumulo.index.FilterHelper._
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner._
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.spatial.BinarySpatialOperator

class STIdxStrategy extends Strategy with Logging with IndexFilterHelpers {

  override def getQueryPlans(query: Query, queryPlanner: QueryPlanner, output: ExplainerOutputType) = {

    val sft             = queryPlanner.sft
    val acc             = queryPlanner.acc
    val version         = queryPlanner.version
    val schema          = queryPlanner.stSchema
    val featureEncoding = queryPlanner.featureEncoding
    val keyPlanner      = IndexSchema.buildKeyPlanner(schema)
    val cfPlanner       = IndexSchema.buildColumnFamilyPlanner(schema)

    output(s"Scanning ST index table for feature type ${sft.getTypeName}")
    output(s"Filter: ${query.getFilter}")

    val dtgField = getDtgFieldName(sft)

    val (geomFilters, otherFilters) = partitionGeom(query.getFilter, sft)
    val (temporalFilters, ecqlFilters) = partitionTemporal(otherFilters, dtgField)

    val ecql = filterListAsAnd(ecqlFilters)

    output(s"Geometry filters: $geomFilters")
    output(s"Temporal filters: $temporalFilters")
    output(s"Other filters: $ecqlFilters")

    val tweakedGeomFilters = geomFilters.map(updateTopologicalFilters(_, sft))

    output(s"Tweaked geom filters are $tweakedGeomFilters")

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = tweakedGeomFilters.flatMap(decomposeToGeometry)

    output(s"GeomsToCover: $geomsToCover")

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    val temporal = extractTemporal(dtgField)(temporalFilters)
    val interval = netInterval(temporal)
    val geometryToCover = netGeom(collectionToCover)

    val filter = buildFilter(geometryToCover, interval)
    // This catches the case when a whole world query slips through DNF/CNF
    // The union on this geometry collection is necessary at the moment but is not true
    // If given spatial predicates like disjoint.
    val ofilter = if (isWholeWorld(geometryToCover)) {
      filterListAsAnd(temporalFilters)
    } else filterListAsAnd(tweakedGeomFilters ++ temporalFilters)

    if (ofilter.isEmpty) {
      logger.warn(s"Querying Accumulo without SpatioTemporal filter.")
    }

    val oint  = IndexSchema.somewhen(interval)

    output(s"STII Filter: ${ofilter.getOrElse("No STII Filter")}")
    output(s"Interval:  ${oint.getOrElse("No interval")}")
    output(s"Filter: ${Option(filter).getOrElse("No Filter")}")

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, sft)

    val stiiIterCfg =
      getSTIIIterCfg(iteratorConfig, query, sft, ofilter, ecql, featureEncoding, version)

    val densityIterCfg = getDensityIterCfg(query, geometryToCover, schema, featureEncoding, sft)

    // set up row ranges and regular expression filter
    val qp = planQuery(filter, iteratorConfig.iterator, output, keyPlanner, cfPlanner)

    val table = acc.getSpatioTemporalTable(sft)
    val iterators = qp.iterators ++ List(Some(stiiIterCfg), densityIterCfg).flatten
    val numThreads = acc.getSuggestedSpatioTemporalThreads(sft)
    val hasDupes = IndexSchema.mayContainDuplicates(sft)
    val kvsToFeatures = queryPlanner.defaultKVsToFeatures(query)
    val res = qp.copy(table = table, iterators = iterators, kvsToFeatures = kvsToFeatures, numThreads = numThreads, hasDuplicates = hasDupes)
    Seq(res)
  }


  private def getSTIIIterCfg(iteratorConfig: IteratorConfig,
                     query: Query,
                     featureType: SimpleFeatureType,
                     stFilter: Option[Filter],
                     ecqlFilter: Option[Filter],
                     featureEncoding: SerializationType,
                     version: Int): IteratorSetting = {
    iteratorConfig.iterator match {
      case IndexOnlyIterator =>
        configureIndexIterator(featureType, query, featureEncoding, stFilter,
          iteratorConfig.transformCoversFilter, version)
      case SpatioTemporalIterator =>
        val isDensity = query.getHints.containsKey(DENSITY_KEY)
        configureSpatioTemporalIntersectingIterator(featureType, query, featureEncoding, stFilter,
          ecqlFilter, isDensity)
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
      query: Query,
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
      getTransformSchema(query).foreach(testType => configureFeatureType(cfg, testType))
    } else {
      // we need to evaluate the original feature before transforming
      // transforms are applied afterwards
      configureFeatureType(cfg, featureType)
      configureTransforms(cfg, query)
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
      query: Query,
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
    configureTransforms(cfg, query)
    configureEcqlFilter(cfg, combinedFilter.map(ECQL.toCQL))
    if (isDensity) {
      cfg.addOption(GEOMESA_ITERATORS_IS_DENSITY_TYPE, "isDensity")
    }
    cfg
  }

  def planQuery(filter: KeyPlanningFilter,
                iter: IteratorChoice,
                output: ExplainerOutputType,
                keyPlanner: KeyPlanner,
                cfPlanner: ColumnFamilyPlanner): BatchScanPlan = {
    output(s"Planning query")

    val indexOnly = iter match {
      case IndexOnlyIterator      => true
      case SpatioTemporalIterator => false
    }

    val keyPlan = keyPlanner.getKeyPlan(filter, indexOnly, output)

    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new org.apache.accumulo.core.data.Range(r.start, r.end))
      case _ => Seq(new org.apache.accumulo.core.data.Range())
    }

    // always try to set a RowID regular expression
    //@TODO this is broken/disabled as a result of the KeyTier
    val iters =
      keyPlan.toRegex match {
        case KeyRegex(regex) => Seq(configureRowRegexIterator(regex))
        case _               => Seq()
      }

    // if you have a list of distinct column-family entries, fetch them
    val cf = columnFamilies match {
      case KeyList(keys) => keys.map { cf => new Text(cf) }
      case _             => Seq()
    }

    // partially fill in, rest will be filled in later
    BatchScanPlan(null, accRanges, iters, cf, null, -1, false)
  }
}

object STIdxStrategy extends StrategyProvider {

  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints) =
    if (spatialFilters(filter) && !isFilterWholeWorld(filter)) {
      val geom = sft.getGeometryDescriptor.getLocalName
      val e1 = filter.asInstanceOf[BinarySpatialOperator].getExpression1
      val e2 = filter.asInstanceOf[BinarySpatialOperator].getExpression2
      checkOrder(e1, e2).filter(_.name == geom).map(_ => StrategyDecision(new STIdxStrategy, -1))
    } else {
      None
    }
}
