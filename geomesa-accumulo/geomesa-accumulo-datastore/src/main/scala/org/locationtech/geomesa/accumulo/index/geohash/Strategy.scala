/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.geohash

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.IteratorSetting
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner._
import org.locationtech.geomesa.accumulo.iterators.legacy.{FEATURE_ENCODING, MapAggregatingIterator, RecordTableIterator, _}
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.Random

object Strategy extends LazyLogging {

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
