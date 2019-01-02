/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.accumulo.index


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.jobs.accumulo._
import org.locationtech.geomesa.jobs.mapreduce.{GeoMesaAccumuloInputFormat, GeoMesaOutputFormat}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

/**
 * Class to copy a schema and all data from one data store to another.
 *
 * Can be used to 'update' geomesa data from older versions. It does this by reading data in the old format
 * and writing it to a new schema which will use the latest format. This way, improvements in serialization,
 * etc can be leveraged for old data.
 */
object SchemaCopyJob {
  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new SchemaCopyJob, args)
    System.exit(result)
  }
}

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
    val sftOut = {
      val dsOut = DataStoreFinder.getDataStore(dsOutParams)
      require(dsOut != null, "The specified output data store could not be created - check your job parameters")
      try {
        var sft = dsOut.getSchema(featureOut)
        if (sft == null) {
          // update the feature name
          if (featureOut == featureIn) {
            sft = sftIn
          } else {
            sft = SimpleFeatureTypes.createType(featureOut, SimpleFeatureTypes.encodeType(sftIn))
          }
          // create the schema in the output datastore
          dsOut.createSchema(sft)
          dsOut.getSchema(featureOut)
        } else {
          sft
        }
      } finally {
        dsOut.dispose()
      }
    }

    require(sftOut != null, "Could not create output type - check your job parameters")

    val conf = new Configuration
    val job = Job.getInstance(conf, s"GeoMesa Schema Copy '${sftIn.getTypeName}' to '${sftOut.getTypeName}'")

    job.setJarByClass(SchemaCopyJob.getClass)
    job.setMapperClass(classOf[PassThroughMapper])
    job.setInputFormatClass(classOf[GeoMesaAccumuloInputFormat])
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)

    val query = new Query(sftIn.getTypeName, ECQL.toFilter(filter))
    GeoMesaAccumuloInputFormat.configure(job, dsInParams, query)

    GeoMesaOutputFormat.configureDataStore(job, dsOutParams)
    GeoMesaConfigurator.setFeatureTypeOut(job.getConfiguration, sftOut.getTypeName)

    val result = job.waitForCompletion(true)

    if (result) 0 else 1
  }

  override def getConf: Configuration = conf

  override def setConf(conf: Configuration): Unit = this.conf = conf
}

class CopyMapper extends Mapper[Text, SimpleFeature, Text, SimpleFeature] {

  type Context = Mapper[Text, SimpleFeature, Text, SimpleFeature]#Context

  private val text: Text = new Text
  private var counter: Counter = null

  private var sftOut: SimpleFeatureType = null

  override protected def setup(context: Context): Unit = {
    counter = context.getCounter("org.locationtech.geomesa", "features-written")
    val dsParams = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    val ds = DataStoreFinder.getDataStore(dsParams)
    sftOut = ds.getSchema(GeoMesaConfigurator.getFeatureTypeOut(context.getConfiguration))
    ds.dispose()
  }

  override protected def cleanup(context: Context): Unit = {

  }

  override def map(key: Text, value: SimpleFeature, context: Context) {
    context.write(text, new ScalaSimpleFeature(sftOut, value.getID, value.getAttributes.toArray))
    counter.increment(1)
  }
}