/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.converter

import java.io.Serializable
import java.util

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.jobs.mapreduce.ConverterInputFormat
import org.locationtech.geomesa.spark.SpatialRDDProvider
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
  * Takes files as inputs and runs them through a SimpleFeatureConverter
  *
  * Parameters:
  *   ``geomesa.converter`` - converter definition as typesafe config string
  *   ``geomesa.converter.inputs`` - input file paths, comma-delimited
  *   ``geomesa.sft`` - simple feature type, as spec string, config string, or environment lookup
  *   ``geomesa.sft.name`` - (optional) simple feature type name
  */
class ConverterSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {

  import ConverterSpatialRDDProvider.{ConverterKey, FeatureNameKey, InputFilesKey, SftKey}

  override def canProcess(params: util.Map[String, Serializable]): Boolean =
    params.containsKey(ConverterKey) && params.containsKey(SftKey) && params.containsKey(InputFilesKey)

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): RDD[SimpleFeature] = {
    val sftName = params.get(FeatureNameKey).orElse(Option(query.getTypeName)).orNull
    val sft = SftArgResolver.getArg(SftArgs(params(SftKey), sftName)) match {
      case Right(s) => s
      case Left(e)  => throw new IllegalArgumentException("Could not resolve simple feature type", e)
    }

    val converterConf = params(ConverterKey)
    try {
      CloseQuietly(SimpleFeatureConverters.build(sft, ConfigFactory.parseString(converterConf)))
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Could not resolve converter", e)
    }

    ConverterInputFormat.setConverterConfig(conf, converterConf)
    ConverterInputFormat.setSft(conf, sft)
    // note: file input format requires a job object, but conf gets copied in job object creation,
    // so we have to copy the file paths back out
    val job = Job.getInstance(conf)
    FileInputFormat.setInputPaths(job, params(InputFilesKey))
    conf.set(FileInputFormat.INPUT_DIR, job.getConfiguration.get(FileInputFormat.INPUT_DIR))

    if (query.getPropertyNames != null && query.getPropertyNames.length > 0) {
      logger.warn("Ignoring query transform - modify converter definition instead")
    }

    if (query.getFilter != null && query.getFilter != Filter.INCLUDE) {
      ConverterInputFormat.setFilter(conf, ECQL.toCQL(query.getFilter))
    }

    val rdd = sc.newAPIHadoopRDD(conf, classOf[ConverterInputFormat], classOf[LongWritable], classOf[SimpleFeature])
    rdd.map(_._2)
  }

  override def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit =
    throw new NotImplementedError("Converter provider is read-only")
}

object ConverterSpatialRDDProvider {
  // converter definition as typesafe config string
  val ConverterKey   = "geomesa.converter"
  // simple feature type name (optional)
  val FeatureNameKey = "geomesa.sft.name"
  // input file paths, comma-delimited
  val InputFilesKey  = "geomesa.converter.inputs"
  // simple feature type, as spec string, config string, or environment lookup
  val SftKey         = "geomesa.sft"
}
