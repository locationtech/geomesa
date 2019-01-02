/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.converter

import java.io.Serializable
import java.util

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.jobs.mapreduce.ConverterInputFormat
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs, SimpleFeatureTypeLoader}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
  * Takes files as inputs and runs them through a SimpleFeatureConverter
  *
  * Parameters:
  *   ``geomesa.converter`` - converter definition as typesafe config string
  *   ``geomesa.converter.inputs`` - input file paths, comma-delimited
  *   ``geomesa.sft`` - simple feature type, as spec string, config string, or environment lookup
  *   ``geomesa.sft.name`` - (optional) simple feature type name
  *   ``geomesa.ingest.type`` Alternative to giving the geomesa.converter and geomesa.sft values:
  *      This option requires that the Converter and SFT can be looked up on this provider's classpath.
  */
class ConverterSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {

  import ConverterSpatialRDDProvider._

  override def canProcess(params: util.Map[String, Serializable]): Boolean =
    ((params.containsKey(ConverterKey) && params.containsKey(SftKey))
      || params.containsKey(IngestTypeKey)) && params.containsKey(InputFilesKey)

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {
    val (sft, converterConf) = computeSftConfig(params, query)

    ConverterInputFormat.setConverterConfig(conf, converterConf)
    ConverterInputFormat.setSft(conf, sft)
    // note: file input format requires a job object, but conf gets copied in job object creation,
    // so we have to copy the file paths back out
    val job = Job.getInstance(conf)
    FileInputFormat.setInputPaths(job, params(InputFilesKey))
    conf.set(FileInputFormat.INPUT_DIR, job.getConfiguration.get(FileInputFormat.INPUT_DIR))
    val queryProperties = query.getPropertyNames
    val sftProperties = sft.getAttributeDescriptors.map{_.getLocalName}
    if (queryProperties != null && queryProperties.nonEmpty && sftProperties != queryProperties.toSeq) {
      logger.debug("Query transform retyping results")
      val modifiedSft = SimpleFeatureTypeBuilder.retype(sft, query.getPropertyNames)
      ConverterInputFormat.setRetypeSft(conf, modifiedSft)
    }

    if (query.getFilter != null && query.getFilter != Filter.INCLUDE) {
      ConverterInputFormat.setFilter(conf, ECQL.toCQL(query.getFilter))
    }

    val rdd = sc.newAPIHadoopRDD(conf, classOf[ConverterInputFormat], classOf[LongWritable], classOf[SimpleFeature])
    SpatialRDD(rdd.map(_._2), sft)
  }

  // TODO:  Move the logic here and in the next function to utils (aka somewhere more general)
  //  GEOMESA-1644 Move logic to retrieve SFT/Converter config from classpath to utils
  private def computeSftConfig(params: Map[String, String], query: Query) = {
    val (sft, converterConf) = lookupSftConfig(params, query)

    // Verify the config before returning.
    try {
      SimpleFeatureConverter(sft, ConfigFactory.parseString(converterConf)).close()
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Could not resolve converter", e)
    }
    (sft, converterConf)
  }

  private def lookupSftConfig(params: Map[String, String], query: Query) = {
    params.get(IngestTypeKey) match {
      case Some(sftName) =>
        // NB: Here we assume that the SFT name and Converter name match.  (And there is no option to rename the SFT.)
        //  Further, it is assumed that they can loaded from the classpath.
        val sft = SimpleFeatureTypeLoader.sftForName(sftName)
          .getOrElse(throw new IllegalArgumentException(s"Could not resolve Simple Feature Type by name for $sftName."))
        val convertConf =
          ConverterConfigLoader.confs
            .getOrElse(sftName, throw new Exception(s"Could not resolve Converter by name for $sftName."))
            .root().render(ConfigRenderOptions.concise())

        (sft, convertConf)

      case _ =>
        // This is the general case where the SFT and Converter config strings are given.
        val sftName = params.get(FeatureNameKey).orElse(Option(query.getTypeName)).orNull

        val sft: SimpleFeatureType = SftArgResolver.getArg(SftArgs(params(SftKey), sftName)) match {
          case Right(s) => s
          case Left(e) => throw new IllegalArgumentException("Could not resolve simple feature type", e)
        }

        val converterConf: String = params(ConverterKey)

        (sft, converterConf)
    }
  }

  override def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit =
    throw new NotImplementedError("Converter provider is read-only")
}

object ConverterSpatialRDDProvider {
  // converter definition as typesafe config string
  val ConverterKey     = "geomesa.converter"
  // simple feature type name (optional)
  val FeatureNameKey   = "geomesa.sft.name"
  // input file paths, comma-delimited
  val InputFilesKey    = "geomesa.converter.inputs"
  // simple feature type, as spec string, config string, or environment lookup
  val SftKey           = "geomesa.sft"
  // Converter name (to be looked up from the classpath).
  val IngestTypeKey = "geomesa.ingest.type"
}
