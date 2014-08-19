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

import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting}
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.Query
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators.{FEATURE_ENCODING, _}
import org.locationtech.geomesa.core.util.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.util.Random

trait Strategy {
  def execute(acc: AccumuloConnectorCreator,
              iqp: QueryPlanner,
              featureType: SimpleFeatureType,
              query: Query,
              filterVisitor: FilterToAccumulo,
              output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]]

  def configureBatchScanner(bs: BatchScanner, qp: QueryPlan) {
    qp.iterators.foreach { i => bs.addScanIterator(i) }
    bs.setRanges(qp.ranges)
    qp.cf.foreach { c => bs.fetchColumnFamily(c) }
  }

  def configureFeatureEncoding(cfg: IteratorSetting, featureEncoder: SimpleFeatureEncoder) {
    cfg.addOption(FEATURE_ENCODING, featureEncoder.getName)
  }

  def configureFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
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
                                              featureEncoder: SimpleFeatureEncoder,
                                              query: Query): IteratorSetting = {
    val cfg = new IteratorSetting(iteratorPriority_SimpleFeatureFilteringIterator,
      "sffilter-" + randomPrintableString(5),
      classOf[SimpleFeatureFilteringIterator])

    cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
    configureFeatureEncoding(cfg, featureEncoder)
    configureTransforms(query,cfg)
    configureFeatureType(cfg, simpleFeatureType)
    ecql.foreach(SimpleFeatureFilteringIterator.setECQLFilter(cfg, _))

    cfg
  }

  def randomPrintableString(length:Int=5) : String = (1 to length).
    map(i => Random.nextPrintableChar()).mkString

  def filterListAsAnd(filters: Seq[Filter]): Option[Filter] = filters match {
    case Nil => None
    case _ => Some(ff.and(filters))
  }
}
