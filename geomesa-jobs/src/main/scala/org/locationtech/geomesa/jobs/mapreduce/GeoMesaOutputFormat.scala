/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.FeatureWriterHelper
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat.GeoMesaRecordWriter
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.io.IOException
import scala.collection.mutable.ArrayBuffer

/**
  * Output format that writes simple features using GeoMesaDataStore's FeatureWriterAppend. Can write only
  * specific indices if desired
  */
class GeoMesaOutputFormat extends OutputFormat[Text, SimpleFeature] {

  import scala.collection.JavaConverters._

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Text, SimpleFeature] = {
    val params = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    val indices = GeoMesaConfigurator.getIndicesOut(context.getConfiguration)
    new GeoMesaRecordWriter(params, indices, context)
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    val params =
      GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
        .asJava.asInstanceOf[java.util.Map[String, java.io.Serializable]]
    if (!DataStoreFinder.getAvailableDataStores.asScala.exists(_.canProcess(params))) {
      throw new IOException("Data store connection parameters are not set")
    }
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter =
    new NullOutputFormat[Text, SimpleFeature]().getOutputCommitter(context)
}

object GeoMesaOutputFormat {

  import scala.collection.JavaConverters._

  object OutputCounters {
    val Group   = "org.locationtech.geomesa.jobs.output"
    val Written = "written"
    val Failed  = "failed"
  }

  /**
    * Configure the data store you will be writing to
    *
    * @param conf conf
    * @param params data store parameters
    * @param sft simple feature type to write, must exist already in the store
    * @param indices indices to write, or all indices
    */
  def setOutput(
      conf: Configuration,
      params: Map[String, String],
      sft: SimpleFeatureType,
      indices: Option[Seq[String]] = None): Unit = {
    GeoMesaConfigurator.setDataStoreOutParams(conf, params)
    GeoMesaConfigurator.setSerialization(conf, sft)
    indices.foreach(GeoMesaConfigurator.setIndicesOut(conf, _))
  }

  /**
   * Helper for java interop
   *
   * @param conf conf
   * @param params data store parameters
   * @param sft simple feature type to write, must exist already in the store
   */
  def setOutputJava(
      conf: Configuration,
      params: java.util.Map[String, String],
      sft: SimpleFeatureType): Unit = {
    setOutput(conf, params.asScala.toMap, sft)
  }

  /**
    * Record writer for GeoMesa datastores.
    *
    * All feature types must exist already in the datastore. The input key is ignored.
    */
  class GeoMesaRecordWriter(params: Map[String, String], indices: Option[Seq[String]], context: TaskAttemptContext)
      extends RecordWriter[Text, SimpleFeature] with LazyLogging {

    private val ds = DataStoreFinder.getDataStore(params.asJava)

    private val writers = ArrayBuffer.empty[FeatureWriter[SimpleFeatureType, SimpleFeature]]
    private val helpers = scala.collection.mutable.Map.empty[String, FeatureWriterHelper]

    private val written = context.getCounter(OutputCounters.Group, OutputCounters.Written)
    private val failed = context.getCounter(OutputCounters.Group, OutputCounters.Failed)

    override def write(key: Text, value: SimpleFeature): Unit = {
      try {
        val sftName = value.getFeatureType.getTypeName
        val helper = helpers.getOrElseUpdate(sftName, createWriter(sftName))
        helper.write(value)
        written.increment(1)
      } catch {
        case e: Exception =>
          logger.error(s"Error writing feature '${DataUtilities.encodeFeature(value)}'", e)
          failed.increment(1)
      }
    }

    private def createWriter(typeName: String): FeatureWriterHelper = {
      val writer = ds match {
        case gm: GeoMesaDataStore[_] =>
          val sft = gm.getSchema(typeName)
          val i = indices match {
            case Some(names) => names.map(gm.manager.index(sft, _, IndexMode.Write))
            case None => gm.manager.indices(sft, mode = IndexMode.Write)
          }
          gm.getIndexWriterAppend(typeName, i)

        case _ =>
          indices.foreach { i =>
            logger.warn(s"Ignoring index config '${i.mkString(",")}' for non-geomesa data store $ds")
          }
          ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
      }
      writers += writer
      FeatureWriterHelper(writer)
    }

    override def close(context: TaskAttemptContext): Unit = {
      CloseQuietly(writers)
      ds.dispose()
    }
  }
}
