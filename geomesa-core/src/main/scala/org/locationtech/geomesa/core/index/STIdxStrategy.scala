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
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, Polygon}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Interval
import org.locationtech.geomesa.core.data.{AccumuloConnectorCreator, SimpleFeatureEncoder}
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.util.{SelfClosingBatchScanner, SelfClosingIterator}
import org.locationtech.geomesa.core.{DEFAULT_SCHEMA_NAME, GEOMESA_ITERATORS_IS_DENSITY_TYPE}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.Literal
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator}

class STIdxStrategy extends Strategy with Logging {

  def execute(acc: AccumuloConnectorCreator,
              iqp: QueryPlanner,
              featureType: SimpleFeatureType,
              query: Query,
              output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val bs = acc.createSTIdxScanner(featureType)
    val qp = buildSTIdxQueryPlan(query, iqp, featureType, output)
    configureBatchScanner(bs, qp)
    // NB: Since we are (potentially) gluing multiple batch scanner iterators together,
    //  we wrap our calls in a SelfClosingBatchScanner.
    SelfClosingBatchScanner(bs)
  }

  def buildSTIdxQueryPlan(query: Query,
                          iqp: QueryPlanner,
                          featureType: SimpleFeatureType,
                          output: ExplainerOutputType) = {
    val schema         = iqp.schema
    val featureEncoder = iqp.featureEncoder
    val keyPlanner     = IndexSchema.buildKeyPlanner(iqp.schema)
    val cfPlanner      = IndexSchema.buildColumnFamilyPlanner(iqp.schema)

    output(s"Scanning ST index table for feature type ${featureType.getTypeName}")

    // TODO: Select only the geometry filters which involve the indexed geometry type.
    // https://geomesa.atlassian.net/browse/GEOMESA-200
    // Simiarly, we should only extract temporal filters for the index date field.
    val (geomFilters, otherFilters) = partitionGeom(query.getFilter)
    val (temporalFilters, ecqlFilters) = partitionTemporal(otherFilters, getDtgFieldName(featureType))

    val ecql = filterListAsAnd(ecqlFilters).map(ECQL.toCQL)

    output(s"The geom filters are $geomFilters.\nThe temporal filters are $temporalFilters.")

    val tweakedGeoms = geomFilters.map(updateTopologicalFilters(_, featureType))

    output(s"Tweaked geom filters are $tweakedGeoms")

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = tweakedGeoms.flatMap {
      case bbox: BBOX =>
        val bboxPoly = bbox.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])
        Seq(bboxPoly)
      case gf: BinarySpatialOperator =>
        extractGeometry(gf)
      case _ => Seq()
    }

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    val temporal = extractTemporal(temporalFilters)
    val interval = netInterval(temporal)
    val geometryToCover = netGeom(collectionToCover)
    val filter = buildFilter(geometryToCover, interval)

    output(s"GeomsToCover $geomsToCover.")

    val ofilter = filterListAsAnd(tweakedGeoms ++ temporalFilters)
    if (ofilter.isEmpty) logger.warn(s"Querying Accumulo without ST filter.")

    val oint  = IndexSchema.somewhen(interval)

    // set up row ranges and regular expression filter
    val qp = planQuery(filter, output, keyPlanner, cfPlanner)

    output("Configuring batch scanner for ST table: \n" +
      s"  Filter ${query.getFilter}\n" +
      s"  STII Filter: ${ofilter.getOrElse("No STII Filter")}\n" +
      s"  Interval:  ${oint.getOrElse("No interval")}\n" +
      s"  Filter: ${Option(filter).getOrElse("No Filter")}\n" +
      s"  ECQL: ${Option(ecql).getOrElse("No ecql")}\n" +
      s"Query: ${Option(query).getOrElse("no query")}.")

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, featureType)

    val stiiIterCfg = getSTIIIterCfg(iteratorConfig, query, featureType, ofilter, schema, featureEncoder)

    val sffiIterCfg = getSFFIIterCfg(iteratorConfig, featureType, ecql, schema, featureEncoder, query)

    val topIterCfg = getTopIterCfg(query, geometryToCover, schema, featureEncoder, featureType)

    qp.copy(iterators = qp.iterators ++ List(Some(stiiIterCfg), sffiIterCfg, topIterCfg).flatten)
  }

  def getSTIIIterCfg(iteratorConfig: IteratorConfig,
                     query: Query,
                     featureType: SimpleFeatureType,
                     ofilter: Option[Filter],
                     schema: String,
                     featureEncoder: SimpleFeatureEncoder): IteratorSetting = {
    iteratorConfig.iterator match {
      case IndexOnlyIterator =>
        configureIndexIterator(ofilter, query, schema, featureEncoder, featureType)
      case SpatioTemporalIterator =>
        val isDensity = query.getHints.containsKey(DENSITY_KEY)
        configureSpatioTemporalIntersectingIterator(ofilter, featureType, schema, isDensity)
    }
  }

  def getSFFIIterCfg(iteratorConfig: IteratorConfig,
                     featureType: SimpleFeatureType,
                     ecql: Option[String],
                     schema: String,
                     featureEncoder: SimpleFeatureEncoder,
                     query: Query): Option[IteratorSetting] = {
    if (iteratorConfig.useSFFI) {
      Some(configureSimpleFeatureFilteringIterator(featureType, ecql, schema, featureEncoder, query))
    } else None
  }

  def getTopIterCfg(query: Query,
                    geometryToCover: Geometry,
                    schema: String,
                    featureEncoder: SimpleFeatureEncoder,
                    featureType: SimpleFeatureType) = {
    if (query.getHints.containsKey(DENSITY_KEY)) {
      val clazz = classOf[DensityIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val width = query.getHints.get(WIDTH_KEY).asInstanceOf[Int]
      val height = query.getHints.get(HEIGHT_KEY).asInstanceOf[Int]
      val polygon = if (geometryToCover == null) null else geometryToCover.getEnvelope.asInstanceOf[Polygon]

      DensityIterator.configure(cfg, polygon, width, height)

      cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
      configureFeatureEncoding(cfg, featureEncoder)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    } else None
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
  def configureIndexIterator(filter: Option[Filter],
                             query: Query,
                             schema: String,
                             featureEncoder: SimpleFeatureEncoder,
                             featureType: SimpleFeatureType): IteratorSetting = {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),classOf[IndexIterator])
    IndexIterator.setOptions(cfg, schema, filter)
    val ab = new AttributeTypeBuilder()
    val builder = new SimpleFeatureTypeBuilder()
    builder.setName(featureType.getName)
    builder.add(featureType.getGeometryDescriptor)
    builder.setDefaultGeometry(featureType.getGeometryDescriptor.getLocalName)
    // dtg attribute is optional -- if it exists add it to the builder
    getDtgDescriptor(featureType).foreach ( builder.add(_) )
    val testType = builder.buildFeatureType()
     // dtg attribute is optional -- if it exists add the pointer to UserData
    getDtgFieldName(featureType).foreach ( testType.getUserData.put(SF_PROPERTY_START_TIME,_) )
    configureFeatureType(cfg, testType)
    configureFeatureEncoding(cfg, featureEncoder)
    cfg
  }

  // returns only the data entries -- no index entries -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureSpatioTemporalIntersectingIterator(filter: Option[Filter],
                                                  featureType: SimpleFeatureType,
                                                  schema: String,
                                                  isDensity: Boolean): IteratorSetting = {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),
      classOf[SpatioTemporalIntersectingIterator])
    SpatioTemporalIntersectingIterator.setOptions(cfg, schema, filter)
    configureFeatureType(cfg, featureType)
    if (isDensity) cfg.addOption(GEOMESA_ITERATORS_IS_DENSITY_TYPE, "isDensity")
    cfg
  }

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

  def planQuery(filter: KeyPlanningFilter, output: ExplainerOutputType, keyPlanner: KeyPlanner, cfPlanner: ColumnFamilyPlanner): QueryPlan = {
    output(s"Planning query")
    val keyPlan = keyPlanner.getKeyPlan(filter, output)
    output(s"Got keyplan ${keyPlan.toString.take(1000)}")

    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new org.apache.accumulo.core.data.Range(r.start, r.end))
      case _ => Seq(new org.apache.accumulo.core.data.Range())
    }

    output(s"Setting ${accRanges.size} ranges.")

    // always try to set a RowID regular expression
    //@TODO this is broken/disabled as a result of the KeyTier
    val iters =
      keyPlan.toRegex match {
        case KeyRegex(regex) => Seq(configureRowRegexIterator(regex))
        case _               => Seq()
      }

    // if you have a list of distinct column-family entries, fetch them
    val cf = columnFamilies match {
      case KeyList(keys) =>
        output(s"Settings ${keys.size} column fams: $keys.")
        keys.map { cf => new Text(cf) }

      case _ =>
        Seq()
    }

    QueryPlan(iters, accRanges, cf)
  }
}

