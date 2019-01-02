/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.accumulo.index


import java.io.File

import com.beust.jcommander.Parameter
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.jobs.accumulo.{GeoMesaArgs, InputCqlArgs, InputDataStoreArgs, InputFeatureArgs}
import org.locationtech.geomesa.jobs.mapreduce.{GeoMesaAccumuloInputFormat, GeoMesaOutputFormat}
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

/**
 * Class to write data to a single index
 *
 * Can be used to back-fill data into a new index without re-writing unchanged indices.
 */
object WriteIndexJob {
  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new WriteIndexJob, args)
    System.exit(result)
  }
}

class WriteIndexArgs(args: Array[String]) extends GeoMesaArgs(args)
    with InputFeatureArgs with InputDataStoreArgs with InputCqlArgs {

  @Parameter(names = Array("--geomesa.index"), description = "Name of index(es) to add - comma-separate or use multiple flags", required = true)
  var indexNames: java.util.List[String] = new java.util.ArrayList[String]()

  override def unparse(): Array[String] = {
    val names = if (indexNames == null || indexNames.isEmpty) {
      Array.empty[String]
    } else {
      indexNames.flatMap(n => Seq("--geomesa.index", n)).toArray
    }
    Array.concat(super[InputFeatureArgs].unparse(),
                 super[InputDataStoreArgs].unparse(),
                 super[InputCqlArgs].unparse(),
                 names)
  }
}

class WriteIndexJob(libjars: Option[(Seq[String], Iterator[() => Seq[File]])] = None) extends Tool with LazyLogging {

  private var conf: Configuration = new Configuration

  override def run(args: Array[String]): Int = {
    val parsedArgs = new WriteIndexArgs(args)
    parsedArgs.parse()

    val featureIn  = parsedArgs.inFeature
    val dsInParams = parsedArgs.inDataStore
    val filter     = Option(parsedArgs.inCql).getOrElse("INCLUDE")

    // validation and initialization - ensure the types exist before launching distributed job
    val (sft, indices) = {
      val dsIn = DataStoreFinder.getDataStore(dsInParams).asInstanceOf[GeoMesaDataStore[_]]
      require(dsIn != null, "The specified input data store could not be created - check your job parameters")
      try {
        val sft = dsIn.getSchema(featureIn)
        require(sft != null, s"The feature '$featureIn' does not exist in the input data store")
        val allIndices = dsIn.manager.indices(sft, IndexMode.Write)
        val indices = parsedArgs.indexNames.map { name =>
          allIndices.find(_.identifier == name).orElse(allIndices.find(_.name == name)).getOrElse {
            throw new IllegalArgumentException(s"Invalid index $name. Valid values are " +
                allIndices.map(_.identifier).sorted.mkString(", "))
          }
        }
        (sft, indices)
      } finally {
        dsIn.dispose()
      }
    }

    val conf = new Configuration
    val jobName = s"GeoMesa Index Job [${sft.getTypeName}] ${indices.map(_.identifier).mkString("[", "][", "]")}"
    val job = Job.getInstance(conf, jobName)

    libjars.foreach { case (jars, path) => JobUtils.setLibJars(job.getConfiguration, jars, path) }

    job.setJarByClass(classOf[WriteIndexJob])
    job.setMapperClass(classOf[PassThroughMapper])
    job.setInputFormatClass(classOf[GeoMesaAccumuloInputFormat])
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)

    val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
    GeoMesaAccumuloInputFormat.configure(job, dsInParams, query)

    // disable writing stats as we're just copying data not creating any
    val dsOutParams = dsInParams ++ Map(AccumuloDataStoreParams.GenerateStatsParam.getName -> "false")
    GeoMesaOutputFormat.configureDataStore(job, dsOutParams)
    GeoMesaConfigurator.setFeatureTypeOut(job.getConfiguration, sft.getTypeName)
    GeoMesaConfigurator.setIndicesOut(job.getConfiguration, indices)

    logger.info("Submitting job - please wait...")
    job.submit()
    logger.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    val result = job.waitForCompletion(true)

    if (result) 0 else 1
  }

  override def getConf: Configuration = conf

  override def setConf(conf: Configuration): Unit = this.conf = conf
}

class PassThroughMapper extends Mapper[Text, SimpleFeature, Text, SimpleFeature] {

  type Context = Mapper[Text, SimpleFeature, Text, SimpleFeature]#Context

  private val text: Text = new Text
  private var counter: Counter = _

  override protected def setup(context: Context): Unit = {
    counter = context.getCounter("org.locationtech.geomesa", "features-written")
  }

  override protected def cleanup(context: Context): Unit = {}

  override def map(key: Text, value: SimpleFeature, context: Context) {
    context.write(text, value)
    counter.increment(1)
  }
}