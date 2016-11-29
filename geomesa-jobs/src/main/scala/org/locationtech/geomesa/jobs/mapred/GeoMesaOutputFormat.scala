/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.mapred

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.Progressable
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.geotools.{GeoMesaDataStore, GeoMesaFeatureWriter}
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object GeoMesaOutputFormat {

  /**
   * Configure the data store you will be writing to.
   */
  def configureDataStore(job: JobConf, dsParams: Map[String, String]): Unit = {
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[GeoMesaDataStore[_, _, _]]
    assert(ds != null, "Invalid data store parameters")
    ds.dispose()

    // set the datastore parameters so we can access them later
    GeoMesaConfigurator.setDataStoreOutParams(job, dsParams)
    GeoMesaConfigurator.setSerialization(job)
  }
}

/**
  * Output format that writes simple features using GeoMesaDataStore's FeatureWriterAppend. Can write only
  * specific indices if desired
  */
class GeoMesaOutputFormat extends OutputFormat[Text, SimpleFeature] {

  override def getRecordWriter(ignored: FileSystem, job: JobConf, name: String, progress: Progressable) = {
    val params  = GeoMesaConfigurator.getDataStoreOutParams(job)
    val indices = GeoMesaConfigurator.getIndicesOut(job)
    new GeoMesaRecordWriter(params, indices, progress)
  }

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf) = {
    val params = GeoMesaConfigurator.getDataStoreOutParams(job)
    if (!DataStoreFinder.getAvailableDataStores.exists(_.canProcess(params))) {
      throw new IOException("Data store connection parameters are not set")
    }
  }
}

/**
 * Record writer for GeoMesa SimpleFeatures.
 *
 * Key is ignored. If the feature type for the given feature does not exist yet, it will be created.
 */
class GeoMesaRecordWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
    (params: Map[String, String], indices: Option[Seq[String]], progress: Progressable)
    extends RecordWriter[Text, SimpleFeature] with LazyLogging {

  val ds = DataStoreFinder.getDataStore(params).asInstanceOf[GeoMesaDataStore[DS, F, W]]

  val sftCache    = scala.collection.mutable.Map.empty[String, SimpleFeatureType]
  val writerCache = scala.collection.mutable.Map.empty[String, GeoMesaFeatureWriter[_, _, _, _]]

  override def write(key: Text, value: SimpleFeature) = {
    val sftName = value.getFeatureType.getTypeName

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
        case Some(names) => names.map(ds.manager.index)
        case None => ds.manager.indices(sft, IndexMode.Write)
      }
      ds.getIndexWriterAppend(sftName, i)
    })

    try {
      val next = writer.next()
      next.getIdentifier.asInstanceOf[FeatureIdImpl].setID(value.getID)
      next.setAttributes(value.getAttributes)
      next.getUserData.putAll(value.getUserData)
      writer.write()
      progress.progress()
    } catch {
      case e: Exception => logger.error(s"Error writing feature '${DataUtilities.encodeFeature(value)}'", e)
    }
  }

  override def close(reporter: Reporter) = {
    writerCache.values.foreach(IOUtils.closeQuietly)
    ds.dispose()
  }
}