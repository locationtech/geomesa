/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2019 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs

import org.apache.commons.csv.{CSVFormat, CSVParser, CSVPrinter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.serializer.WritableSerialization
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
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
  private val sftNameKey       = s"$prefix.sft"
  private val sftKey           = s"$prefix.sft.schema"
  private val tableKey         = s"$prefix.table"
  private val transformsKey    = s"$prefix.transforms.schema"
  private val transformNameKey = s"$prefix.transforms.name"
  private val propertiesKey    = s"$prefix.transforms.props"
  private val indexInKey       = s"$prefix.in.indices"
  private val sftKeyOut        = s"$prefix.out.sft"
  private val indicesOutKey    = s"$prefix.out.indices"
  private val desiredSplits    = s"$prefix.mapreduce.split.count.strongHint"
  private val serializersKey   = "io.serializations"

  private val writableSerialization      = classOf[WritableSerialization].getName
  private val simpleFeatureSerialization = classOf[SimpleFeatureSerialization].getName

  // set/get the connection parameters for an input format
  def setDataStoreInParams(conf: Configuration, params: Map[String, String]): Unit =
    params.foreach { case (key, value) => if (value != null) conf.set(s"$dsInParams$key", value) }
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
    conf.set(sftNameKey, featureType)
  def getFeatureType(job: Job): String = getFeatureType(job.getConfiguration)
  def getFeatureType(conf: Configuration): String = conf.get(sftNameKey)

  // set/get the feature type
  def setSchema(conf: Configuration, sft: SimpleFeatureType): Unit = {
    conf.set(sftNameKey, sft.getTypeName)
    conf.set(sftKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
  }
  def getSchema(job: Job): SimpleFeatureType = getSchema(job.getConfiguration)
  def getSchema(conf: Configuration): SimpleFeatureType = {
    val typeName = conf.get(sftNameKey)
    val schema = conf.get(sftKey)
    SimpleFeatureTypes.createType(typeName, schema)
  }

  // set/get the feature type name
  def setFeatureTypeOut(conf: Configuration, featureType: String): Unit =
    conf.set(sftKeyOut, featureType)
  def getFeatureTypeOut(job: Job): String = getFeatureTypeOut(job.getConfiguration)
  def getFeatureTypeOut(conf: Configuration): String = conf.get(sftKeyOut)

  def setTable(conf: Configuration, featureType: String): Unit =
    conf.set(tableKey, featureType)
  def getTable(job: Job): String = getTable(job.getConfiguration)
  def getTable(conf: Configuration): String = conf.get(tableKey)

  def setIndexIn(conf: Configuration, index: GeoMesaFeatureIndex[_, _]): Unit =
    conf.set(indexInKey, index.identifier)
  def getIndexIn(job: Job): String = getIndexIn(job.getConfiguration)
  def getIndexIn(conf: Configuration): String = conf.get(indexInKey)

  def setIndicesOut(conf: Configuration, indices: Seq[GeoMesaFeatureIndex[_, _]]): Unit =
    conf.set(indicesOutKey, indices.map(_.identifier).mkString(","))
  def getIndicesOut(job: Job): Option[Seq[String]] = getIndicesOut(job.getConfiguration)
  def getIndicesOut(conf: Configuration): Option[Seq[String]] =
    Option(conf.get(indicesOutKey)).map(_.split(","))

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

  def setPropertyNames(conf: Configuration, properties: Array[String]): Unit = {
    if (properties != null) {
      val sb = new java.lang.StringBuilder
      val printer = new CSVPrinter(sb, CSVFormat.DEFAULT)
      properties.foreach(printer.print)
      conf.set(propertiesKey, sb.toString)
    }
  }
  def getPropertyNames(job: Job): Option[Array[String]] = getPropertyNames(job.getConfiguration)
  def getPropertyNames(conf: Configuration): Option[Array[String]] =
    Option(conf.get(propertiesKey)).flatMap { strings =>
      val parser = CSVParser.parse(strings, CSVFormat.DEFAULT)
      val iter = parser.iterator
      if (iter.hasNext) { Some(iter.next.iterator.toArray[String]) } else { None }
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
