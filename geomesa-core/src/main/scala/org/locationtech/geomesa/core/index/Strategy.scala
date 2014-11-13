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

import com.vividsolutions.jts.geom.{Geometry, Polygon}
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting}
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.Query
import org.joda.time.Interval
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators.{FEATURE_ENCODING, _}
import org.locationtech.geomesa.core.util.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.Random

trait Strategy {
  def execute(acc: AccumuloConnectorCreator,
              iqp: QueryPlanner,
              featureType: SimpleFeatureType,
              query: Query,
              output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]]

  def configureBatchScanner(bs: BatchScanner, qp: QueryPlan) {
    qp.iterators.foreach { i => bs.addScanIterator(i) }
    bs.setRanges(qp.ranges)
    qp.cf.foreach { c => bs.fetchColumnFamily(c) }
  }

  def configureFeatureEncoding(cfg: IteratorSetting, featureEncoding: FeatureEncoding) {
    cfg.addOption(FEATURE_ENCODING, featureEncoding.toString)
  }

  def configureFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  def configureFeatureTypeName(cfg: IteratorSetting, featureType: String) {
    cfg.addOption(GEOMESA_ITERATORS_SFT_NAME, featureType)
  }

  def configureAttributeName(cfg: IteratorSetting, attributeName: String) {
    cfg.addOption(GEOMESA_ITERATORS_ATTRIBUTE_NAME, attributeName)
  }

  // returns the SimpleFeatureType for the query's transform
  def transformedSimpleFeatureType(query: Query): Option[SimpleFeatureType] = {
    Option(query.getHints.get(TRANSFORM_SCHEMA)).map {_.asInstanceOf[SimpleFeatureType]}
  }

  // store transform information into an Iterator's settings
  def configureTransforms(query:Query,cfg: IteratorSetting) =
    for {
      transformOpt  <- Option(query.getHints.get(TRANSFORMS))
      transform     = transformOpt.asInstanceOf[String]
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM, transform)
      sfType        <- transformedSimpleFeatureType(query)
      encodedSFType = SimpleFeatureTypes.encodeType(sfType)
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM_SCHEMA, encodedSFType)
    } yield Unit

  // assumes that it receives an iterator over data-only entries, and aggregates
  // the values into a map of attribute, value pairs
  def configureSimpleFeatureFilteringIterator(simpleFeatureType: SimpleFeatureType,
                                              ecql: Option[String],
                                              schema: String,
                                              featureEncoding: FeatureEncoding,
                                              query: Query): IteratorSetting = {
    val cfg = new IteratorSetting(iteratorPriority_SimpleFeatureFilteringIterator,
      "sffilter-" + randomPrintableString(5),
      classOf[SimpleFeatureFilteringIterator])

    cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
    configureFeatureEncoding(cfg, featureEncoding)
    configureTransforms(query,cfg)
    configureFeatureType(cfg, simpleFeatureType)
    ecql.foreach(SimpleFeatureFilteringIterator.setECQLFilter(cfg, _))

    cfg
  }

  def randomPrintableString(length:Int=5) : String = (1 to length).
    map(i => Random.nextPrintableChar()).mkString

  def getSFFIIterCfg(iteratorConfig: IteratorConfig,
                     featureType: SimpleFeatureType,
                     ecql: Option[String],
                     schema: String,
                     featureEncoding: FeatureEncoding,
                     query: Query): Option[IteratorSetting] = {
    if (iteratorConfig.useSFFI) {
      Some(configureSimpleFeatureFilteringIterator(featureType, ecql, schema, featureEncoding, query))
    } else None
  }

  def getTopIterCfg(query: Query,
                    geometryToCover: Geometry,
                    schema: String,
                    featureEncoding: FeatureEncoding,
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
      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    }
    else if (query.getHints.containsKey(TEMPORAL_DENSITY_KEY)){
      val clazz = classOf[TemporalDensityIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val interval = query.getHints.get(TIME_INTERVAL_KEY).asInstanceOf[Interval]
      val buckets = query.getHints.get(TIME_BUCKETS_KEY).asInstanceOf[Int]

      TemporalDensityIterator.configure(cfg, interval, buckets)

      cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    } else None
  }
}
