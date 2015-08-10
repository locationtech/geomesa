/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.serializer.WritableSerialization
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.jobs.mapreduce.SimpleFeatureSerialization
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

/**
 * Common place for setting and getting values out of the mapreduce config
 */
object GeoMesaConfigurator {

  private val prefix         = "org.locationtech.geomesa"

  private val dsInParams     = s"$prefix.params.in."
  private val dsOutParams    = s"$prefix.params.out."
  private val dsInRegex      = dsInParams.replaceAll("\\.", "\\.") + ".+"
  private val dsOutRegex     = dsOutParams.replaceAll("\\.", "\\.") + ".+"
  private val dsInSubstring  = dsInParams.length
  private val dsOutSubstring = dsOutParams.length

  private val filterKey        = s"$prefix.filter"
  private val sftKey           = s"$prefix.sft"
  private val transformsKey    = s"$prefix.transforms.schema"
  private val transformNameKey = s"$prefix.transforms.name"
  private val desiredSplits    = s"$prefix.mapreduce.split.count.strongHint"
  private val serializersKey   = "io.serializations"

  private val writableSerialization      = classOf[WritableSerialization].getName
  private val simpleFeatureSerialization = classOf[SimpleFeatureSerialization].getName

  // set/get the connection parameters for an input format
  def setDataStoreInParams(conf: Configuration, params: Map[String, String]): Unit =
    params.foreach { case (key, value) => conf.set(s"$dsInParams$key", value) }
  def getDataStoreInParams(job: Job): Map[String, String] =
    getDataStoreInParams(job.getConfiguration)
  def getDataStoreInParams(conf: Configuration): Map[String, String] =
    conf.getValByRegex(dsInRegex).map { case (key, value) => (key.substring(dsInSubstring), value) }.toMap

  // set/get the connection parameters for an output format
  def setDataStoreOutParams(conf: Configuration, params: Map[String, String]): Unit =
    params.foreach { case (key, value) => conf.set(s"$dsOutParams$key", value) }
  def getDataStoreOutParams(job: Job): Map[String, String] =
    getDataStoreOutParams(job.getConfiguration)
  def getDataStoreOutParams(conf: Configuration): Map[String, String] =
    conf.getValByRegex(dsOutRegex).map { case (key, value) => (key.substring(dsOutSubstring), value) }.toMap

  // set/get the feature type name
  def setFeatureType(conf: Configuration, featureType: String): Unit =
    conf.set(sftKey, featureType)
  def getFeatureType(job: Job): String = getFeatureType(job.getConfiguration)
  def getFeatureType(conf: Configuration): String = conf.get(sftKey)

  /**
   * Configure the number of desired splits. This should be called with the final intended
   * value. In general, you should use the number of shards * a guess about the number of
   * partitions or splits you want per shard. The default is 2. Behavior is undefined for
   * numbers less than 1.
   *
   * @param conf
   * @param countOfSplits
   */
  def setDesiredSplits(conf: Configuration, countOfSplits : Int) : Unit =
    conf.setInt(desiredSplits, countOfSplits)
  def getDesiredSplits(job : Job): Int = getDesiredSplits(job.getConfiguration)
  def getDesiredSplits(conf : Configuration): Int = conf.getInt(desiredSplits, -1)

  // set/get the cql filter
  def setFilter(conf: Configuration, filter: String): Unit = conf.set(filterKey, filter)
  def getFilter(job: Job): Option[String] = getFilter(job.getConfiguration)
  def getFilter(conf: Configuration): Option[String] = Option(conf.get(filterKey))

  // set/get query transforms
  def setTransformSchema(conf: Configuration, schema: SimpleFeatureType): Unit = {
    conf.set(transformNameKey, schema.getTypeName)
    conf.set(transformsKey, SimpleFeatureTypes.encodeType(schema))
  }
  def getTransformSchema(job: Job): Option[SimpleFeatureType] = getTransformSchema(job.getConfiguration)
  def getTransformSchema(conf: Configuration): Option[SimpleFeatureType] =
    for {
      transformName   <- Option(conf.get(transformNameKey))
      transformSchema <- Option(conf.get(transformsKey))
    } yield {
      SimpleFeatureTypes.createType(transformName, transformSchema)
    }

  // add our simple feature serialization to the config
  def setSerialization(conf: Configuration): Unit = {
    val existing = conf.get(serializersKey)
    val serializers = if (existing == null) {
      serializationString
    } else if (!existing.contains(simpleFeatureSerialization)) {
      Seq(existing, simpleFeatureSerialization).mkString(",")
    } else {
      existing
    }
    conf.set(serializersKey, serializers)
  }

  val serializationString: String = s"$writableSerialization,$simpleFeatureSerialization"

}
