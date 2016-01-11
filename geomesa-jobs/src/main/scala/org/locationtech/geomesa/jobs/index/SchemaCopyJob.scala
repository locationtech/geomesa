/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.index


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.jobs.mapreduce.{GeoMesaInputFormat, GeoMesaOutputFormat}
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

    val parsedArgs = new GeoMesaArgs(args) with InputFeatureArgs with InputDataStoreArgs with InputCqlArgs
                         with OutputFeatureOptionalArgs with OutputDataStoreArgs

    val featureIn   = parsedArgs.inFeature
    val featureOut  = Option(parsedArgs.outFeature).getOrElse(featureIn)

    val dsInParams  = parsedArgs.inDataStore
    val dsOutParams = parsedArgs.outDataStore
    val filter      = Option(parsedArgs.inCql).getOrElse("INCLUDE")

    // validation and initialization - ensure the types exist before launching distributed job
    val sftIn = {
      val dsIn = DataStoreFinder.getDataStore(dsInParams)
      require(dsIn != null, "The specified input data store could not be created - check your job parameters")
      val sft = dsIn.getSchema(featureIn)
      require(sft != null, s"The feature '$featureIn' does not exist in the input data store")
      sft
    }
    val sftOut = {
      val dsOut = DataStoreFinder.getDataStore(dsOutParams)
      require(dsOut != null, "The specified output data store could not be created - check your job parameters")
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
    }

    require(sftOut != null, "Could not create output type - check your job parameters")

    val conf = new Configuration
    val job = Job.getInstance(conf, s"GeoMesa Schema Copy '${sftIn.getTypeName}' to '${sftOut.getTypeName}'")

    job.setJarByClass(SchemaCopyJob.getClass)
    job.setMapperClass(classOf[CopyMapper])
    job.setInputFormatClass(classOf[GeoMesaInputFormat])
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)

    val query = new Query(sftIn.getTypeName, ECQL.toFilter(filter))
    GeoMesaInputFormat.configure(job, dsInParams, query)

    GeoMesaOutputFormat.configureDataStore(job, dsOutParams)
    GeoMesaConfigurator.setFeatureTypeOut(job.getConfiguration, sftOut.getTypeName)

    val result = job.waitForCompletion(true)

    System.exit(if (result) 0 else 1)
  }
}

class CopyMapper extends Mapper[Text, SimpleFeature, Text, SimpleFeature] {

  type Context = Mapper[Text, SimpleFeature, Text, SimpleFeature]#Context

  private val text: Text = new Text
  private var counter: Counter = null

  private var sftOut: SimpleFeatureType = null

  override protected def setup(context: Context): Unit = {
    counter = context.getCounter("org.locationtech.geomesa", "features-writtern")
    val dsParams = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    val ds = DataStoreFinder.getDataStore(dsParams)
    sftOut = ds.getSchema(GeoMesaConfigurator.getFeatureTypeOut(context.getConfiguration))
  }

  override protected def cleanup(context: Context): Unit = {

  }

  override def map(key: Text, value: SimpleFeature, context: Context) {
    context.write(text, new ScalaSimpleFeature(value.getID, sftOut, value.getAttributes.toArray))
    counter.increment(1)
  }
}