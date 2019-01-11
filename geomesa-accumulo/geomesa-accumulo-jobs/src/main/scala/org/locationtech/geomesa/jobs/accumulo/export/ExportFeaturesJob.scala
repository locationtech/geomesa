/***********************************************************************
 * Crown Copyright (c) 2017-2019 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.jobs.accumulo.export

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.jobs.accumulo._
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

/**
 * Class to export features to HDFS. Currently just writes SFTs as plain text using toString.
 *
 */
object ExportFeaturesJob {
  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new ExportFeaturesJob, args)
    System.exit(result)
  }
}

class ExportFeaturesArgs(args: Array[String]) extends GeoMesaArgs(args)
    with InputFeatureArgs with InputDataStoreArgs with InputCqlArgs
    with OutputHdfsArgs {

  override def unparse(): Array[String] = {
    Array.concat(super[InputFeatureArgs].unparse(),
                 super[InputDataStoreArgs].unparse(),
                 super[InputCqlArgs].unparse(),
                 super[OutputHdfsArgs].unparse()
    )
  }
}

class ExportFeaturesJob extends Tool {

  private var conf: Configuration = new Configuration

  override def run(args: Array[String]): Int = {

    // Parse and extract args
    val parsedArgs = new ExportFeaturesArgs(args)
    parsedArgs.parse()

    val featureIn   = parsedArgs.inFeature
    val dsInParams  = parsedArgs.inDataStore
    val filter      = Option(parsedArgs.inCql).getOrElse("INCLUDE")
    val hdfsPath    = parsedArgs.outHdfs

    // validation and initialization - ensure the types exist before launching distributed job
    val sftIn = {
      val dsIn = DataStoreFinder.getDataStore(dsInParams)
      require(dsIn != null, "The specified input data store could not be created - check your job parameters")
      try {
        val sft = dsIn.getSchema(featureIn)
        require(sft != null, s"The feature '$featureIn' does not exist in the input data store")
        sft
      } finally {
        dsIn.dispose()
      }
    }

    val conf = new Configuration
    val job = Job.getInstance(conf, s"GeoMesa Export '${sftIn.getTypeName}' to '${hdfsPath}'")

    // Set up Hadoop job
    job.setJarByClass(ExportFeaturesJob.getClass)
    job.setMapperClass(classOf[ExportMapper])
    job.setInputFormatClass(classOf[GeoMesaAccumuloInputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setNumReduceTasks(0)
    FileOutputFormat.setOutputPath(job, new Path(hdfsPath))

    val query = new Query(sftIn.getTypeName, ECQL.toFilter(filter))
    GeoMesaAccumuloInputFormat.configure(job, dsInParams, query)

    val result = job.waitForCompletion(true)

    if (result) 0 else 1
  }

  override def getConf: Configuration = conf

  override def setConf(conf: Configuration): Unit = this.conf = conf
}

class ExportMapper extends Mapper[Text, SimpleFeature, Text, Text] {

  type Context = Mapper[Text, SimpleFeature, Text, Text]#Context

  private val text: Text = new Text
  private var counter: Counter = null

  override protected def setup(context: Context): Unit = {
    counter = context.getCounter("org.locationtech.geomesa", "features-exported")
  }

  override protected def cleanup(context: Context): Unit = {}

  override def map(key: Text, value: SimpleFeature, context: Context) {
    context.write(text, new Text(value.toString))
    counter.increment(1)
  }
}
