/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.jobs.index


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.jobs._
import org.locationtech.geomesa.accumulo.jobs.index.SchemaCopyJob.SchemaCopyArgs
import org.locationtech.geomesa.accumulo.jobs.index.WriteIndexJob.PassThroughMapper
import org.locationtech.geomesa.accumulo.jobs.mapreduce.GeoMesaAccumuloInputFormat
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithStore

import scala.collection.JavaConverters._

/**
 * Class to copy a schema and all data from one data store to another.
 *
 * Can be used to 'update' geomesa data from older versions. It does this by reading data in the old format
 * and writing it to a new schema which will use the latest format. This way, improvements in serialization,
 * etc can be leveraged for old data.
 */
object SchemaCopyJob {

  private val CopySchemaNameKey = "org.locationtech.geomesa.copy.name"
  private val CopySchemaSpecKey = "org.locationtech.geomesa.copy.spec"

  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new SchemaCopyJob, args)
    System.exit(result)
  }

  private def setCopySchema(conf: Configuration, sft: SimpleFeatureType): Unit = {
    conf.set(CopySchemaNameKey, sft.getTypeName)
    conf.set(CopySchemaSpecKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
  }

  private def getCopySchema(conf: Configuration): SimpleFeatureType =
    SimpleFeatureTypes.createType(conf.get(CopySchemaNameKey), conf.get(CopySchemaSpecKey))

  class SchemaCopyArgs(args: Array[String]) extends GeoMesaArgs(args)
      with InputFeatureArgs with InputDataStoreArgs with InputCqlArgs
      with OutputFeatureOptionalArgs with OutputDataStoreArgs {

    override def unparse(): Array[String] = {
      Array.concat(super[InputFeatureArgs].unparse(),
        super[InputDataStoreArgs].unparse(),
        super[InputCqlArgs].unparse(),
        super[OutputFeatureOptionalArgs].unparse(),
        super[OutputDataStoreArgs].unparse()
      )
    }
  }

  class CopyMapper extends Mapper[Text, SimpleFeature, Text, SimpleFeature] {

    type Context = Mapper[Text, SimpleFeature, Text, SimpleFeature]#Context

    private val text: Text = new Text
    private var counter: Counter = _

    private var sftOut: SimpleFeatureType = _

    override protected def setup(context: Context): Unit = {
      counter = context.getCounter("org.locationtech.geomesa", "features-written")
      sftOut = getCopySchema(context.getConfiguration)
    }

    override protected def cleanup(context: Context): Unit = {}

    override def map(key: Text, value: SimpleFeature, context: Context) {
      context.write(text, ScalaSimpleFeature.copy(sftOut, value))
      counter.increment(1)
    }
  }
}

class SchemaCopyJob extends Tool {

  private var conf: Configuration = new Configuration

  override def run(args: Array[String]): Int = {
    val parsedArgs = new SchemaCopyArgs(args)
    parsedArgs.parse()

    val featureIn   = parsedArgs.inFeature
    val featureOut  = Option(parsedArgs.outFeature).getOrElse(featureIn)

    val dsInParams  = parsedArgs.inDataStore
    val dsOutParams = parsedArgs.outDataStore
    val filter      = Option(parsedArgs.inCql).getOrElse("INCLUDE")

    // validation and initialization - ensure the types exist before launching distributed job
    val (sftIn, plan) = WithStore[AccumuloDataStore](dsOutParams) { dsIn =>
      require(dsIn != null, "The specified input data store could not be created - check your job parameters")
      val sft = dsIn.getSchema(featureIn)
      require(sft != null, s"The feature '$featureIn' does not exist in the input data store")
      val plan = AccumuloJobUtils.getSingleQueryPlan(dsIn, new Query(sft.getTypeName, ECQL.toFilter(filter)))
      (sft, plan)
    }

    val sftOut = WithStore[AccumuloDataStore](dsOutParams) { dsOut =>
      require(dsOut != null, "The specified output data store could not be created - check your job parameters")
      var sft = dsOut.getSchema(featureOut)
      if (sft != null) { sft } else {
        // update the feature name
        if (featureOut == featureIn) {
          sft = sftIn
        } else {
          sft = SimpleFeatureTypes.createType(featureOut, SimpleFeatureTypes.encodeType(sftIn))
        }
        // create the schema in the output datastore
        dsOut.createSchema(sft)
        dsOut.getSchema(featureOut)
      }
    }
    require(sftOut != null, "Could not create output type - check your job parameters")

    val conf = new Configuration()
    val job = Job.getInstance(conf, s"GeoMesa Schema Copy '${sftIn.getTypeName}' to '${sftOut.getTypeName}'")

    job.setJarByClass(SchemaCopyJob.getClass)
    job.setMapperClass(classOf[PassThroughMapper])
    job.setInputFormatClass(classOf[GeoMesaAccumuloInputFormat])
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)

    GeoMesaAccumuloInputFormat.configure(job.getConfiguration, dsInParams.asJava, plan)
    GeoMesaOutputFormat.setOutput(job.getConfiguration, dsOutParams, sftOut)
    SchemaCopyJob.setCopySchema(job.getConfiguration, sftOut)

    val result = job.waitForCompletion(true)

    if (result) 0 else 1
  }

  override def getConf: Configuration = conf

  override def setConf(conf: Configuration): Unit = this.conf = conf


}
