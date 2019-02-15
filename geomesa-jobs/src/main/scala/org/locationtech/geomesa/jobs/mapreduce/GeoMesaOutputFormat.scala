/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object GeoMesaOutputFormat {

  object Counters {
    val Group   = "org.locationtech.geomesa.jobs.output"
    val Written = "written"
    val Failed  = "failed"
  }

  /**
   * Configure the data store you will be writing to.
   */
  def configureDataStore(job: Job, dsParams: Map[String, String]): Unit = {
    val ds = DataStoreFinder.getDataStore(dsParams)
    assert(ds != null, "Invalid data store parameters")
    ds.dispose()

    // set the datastore parameters so we can access them later
    val conf = job.getConfiguration
    GeoMesaConfigurator.setDataStoreOutParams(conf, dsParams)
    GeoMesaConfigurator.setSerialization(conf)
  }
}

/**
  * Output format that writes simple features using GeoMesaDataStore's FeatureWriterAppend. Can write only
  * specific indices if desired
  */
class GeoMesaOutputFormat extends OutputFormat[Text, SimpleFeature] {

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Text, SimpleFeature] = {
    val params  = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    val indices = GeoMesaConfigurator.getIndicesOut(context.getConfiguration)
    new GeoMesaRecordWriter(params, indices, context)
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    val params = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    if (!DataStoreFinder.getAvailableDataStores.exists(_.canProcess(params))) {
      throw new IOException("Data store connection parameters are not set")
    }
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter =
    new NullOutputFormat[Text, SimpleFeature]().getOutputCommitter(context)
}

/**
 * Record writer for GeoMesa SimpleFeatures.
 *
 * Key is ignored. If the feature type for the given feature does not exist yet, it will be created.
 */
class GeoMesaRecordWriter[DS <: GeoMesaDataStore[DS]](params: Map[String, String],
                                                      indices: Option[Seq[String]],
                                                      context: TaskAttemptContext)
    extends RecordWriter[Text, SimpleFeature] with LazyLogging {

  val ds: DS = DataStoreFinder.getDataStore(params).asInstanceOf[DS]

  val sftCache    = scala.collection.mutable.Map.empty[String, SimpleFeatureType]
  val writerCache = scala.collection.mutable.Map.empty[String, SimpleFeatureWriter]

  val written: Counter = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written)
  val failed: Counter = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed)

  override def write(key: Text, value: SimpleFeature): Unit = {
    val sftName = value.getFeatureType.getTypeName
    // TODO we shouldn't serialize the sft with each feature
    // ensure that the type has been created if we haven't seen it before
    val sft = sftCache.getOrElseUpdate(sftName, {
      // schema operations are thread-safe
      val existing = ds.getSchema(sftName)
      if (existing == null) {
        ds.createSchema(value.getFeatureType)
        ds.getSchema(sftName)
      } else {
        existing
      }
    })

    val writer = writerCache.getOrElseUpdate(sftName, {
      val i = indices match {
        case Some(names) => names.map(ds.manager.index(sft, _, IndexMode.Write))
        case None => ds.manager.indices(sft, mode = IndexMode.Write)
      }
      ds.getIndexWriterAppend(sftName, i)
    })

    try {
      val next = writer.next()
      next.getIdentifier.asInstanceOf[FeatureIdImpl].setID(value.getID)
      next.setAttributes(value.getAttributes)
      next.getUserData.putAll(value.getUserData)
      writer.write()
      written.increment(1)
    } catch {
      case e: Exception =>
        logger.error(s"Error writing feature '${DataUtilities.encodeFeature(value)}'", e)
        failed.increment(1)
    }
  }

  override def close(context: TaskAttemptContext): Unit = {
    writerCache.values.foreach(v => CloseQuietly(v))
    ds.dispose()
  }
}