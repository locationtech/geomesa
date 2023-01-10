/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.jobs.index


import com.beust.jcommander.Parameter
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.jobs._
import org.locationtech.geomesa.accumulo.jobs.index.WriteIndexJob.{PassThroughMapper, WriteIndexArgs}
import org.locationtech.geomesa.accumulo.jobs.mapreduce.GeoMesaAccumuloInputFormat
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.WithStore

import java.io.File
import scala.collection.JavaConverters._

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

  class WriteIndexArgs(args: Array[String]) extends GeoMesaArgs(args)
      with InputFeatureArgs with InputDataStoreArgs with InputCqlArgs {

    @Parameter(names = Array("--geomesa.index"), description = "Name of index(es) to add - comma-separate or use multiple flags", required = true)
    var indexNames: java.util.List[String] = new java.util.ArrayList[String]()

    override def unparse(): Array[String] = {
      val names = if (indexNames == null || indexNames.isEmpty) {
        Array.empty[String]
      } else {
        indexNames.asScala.flatMap(n => Seq("--geomesa.index", n)).toArray
      }
      Array.concat(super[InputFeatureArgs].unparse(),
        super[InputDataStoreArgs].unparse(),
        super[InputCqlArgs].unparse(),
        names)
    }
  }

  class PassThroughMapper extends Mapper[Text, SimpleFeature, Text, SimpleFeature] {

    type Context = Mapper[Text, SimpleFeature, Text, SimpleFeature]#Context

    private val text: Text = new Text
    private var counter: Counter = _

    override protected def setup(context: Context): Unit =
      counter = context.getCounter("org.locationtech.geomesa", "features-written")

    override protected def cleanup(context: Context): Unit = {}

    override def map(key: Text, value: SimpleFeature, context: Context) {
      context.write(text, value)
      counter.increment(1)
    }
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
    val (sft, indices, plan) = WithStore[AccumuloDataStore](dsInParams) { dsIn =>
      require(dsIn != null, "The specified input data store could not be created - check your job parameters")
      val sft = dsIn.getSchema(featureIn)
      require(sft != null, s"The feature '$featureIn' does not exist in the input data store")
      val allIndices = dsIn.manager.indices(sft, IndexMode.Write)
      val indices = parsedArgs.indexNames.asScala.map { name =>
        allIndices.find(_.identifier == name).orElse(allIndices.find(_.name == name)).getOrElse {
          throw new IllegalArgumentException(s"Invalid index $name. Valid values are " +
              allIndices.map(_.identifier).sorted.mkString(", "))
        }
      }
      val plan = AccumuloJobUtils.getSingleQueryPlan(dsIn, new Query(sft.getTypeName, ECQL.toFilter(filter)))
      (sft, indices.map(_.identifier), plan)
    }

    val conf = new Configuration
    val jobName = s"GeoMesa Index Job [${sft.getTypeName}] ${indices.mkString("[", "][", "]")}"
    val job = Job.getInstance(conf, jobName)

    libjars.foreach { case (jars, path) => JobUtils.setLibJars(job.getConfiguration, jars, path) }

    job.setJarByClass(classOf[WriteIndexJob])
    job.setMapperClass(classOf[PassThroughMapper])
    job.setInputFormatClass(classOf[GeoMesaAccumuloInputFormat])
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)

    GeoMesaAccumuloInputFormat.configure(job.getConfiguration, dsInParams.asJava, plan)

    // disable writing stats as we're just copying data not creating any
    val dsOutParams = dsInParams ++ Map(AccumuloDataStoreParams.GenerateStatsParam.getName -> "false")
    GeoMesaOutputFormat.setOutput(job.getConfiguration, dsOutParams, sft, Some(indices.toSeq))

    logger.info("Submitting job - please wait...")
    job.submit()
    logger.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    val result = job.waitForCompletion(true)

    if (result) 0 else 1
  }

  override def getConf: Configuration = conf

  override def setConf(conf: Configuration): Unit = this.conf = conf
}
