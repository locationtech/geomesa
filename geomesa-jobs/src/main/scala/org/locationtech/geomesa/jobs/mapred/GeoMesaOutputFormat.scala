/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.mapred

import java.io.IOException

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.Progressable
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory, AccumuloFeatureWriter}
import org.locationtech.geomesa.accumulo.index.IndexValueEncoder
import org.locationtech.geomesa.features.{SimpleFeatureSerializer, SimpleFeatureSerializers}
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object GeoMesaOutputFormat {

  /**
   * Configure the data store you will be writing to.
   */
  def configureDataStore(job: JobConf, dsParams: Map[String, String]): Unit = {

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

    assert(ds != null, "Invalid data store parameters")

    // set up the underlying accumulo input format
    val user = AccumuloDataStoreFactory.params.userParam.lookUp(dsParams).asInstanceOf[String]
    val password = AccumuloDataStoreFactory.params.passwordParam.lookUp(dsParams).asInstanceOf[String]
    AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(password.getBytes()))

    val instance = AccumuloDataStoreFactory.params.instanceIdParam.lookUp(dsParams).asInstanceOf[String]
    val zookeepers = AccumuloDataStoreFactory.params.zookeepersParam.lookUp(dsParams).asInstanceOf[String]
    AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers)

    AccumuloOutputFormat.setCreateTables(job, false)

    // also set the datastore parameters so we can access them later
    GeoMesaConfigurator.setDataStoreOutParams(job, dsParams)
    GeoMesaConfigurator.setSerialization(job)
  }

  /**
   * Configure the batch writer options used by accumulo.
   */
  def configureBatchWriter(job: JobConf, writerConfig: BatchWriterConfig): Unit =
    AccumuloOutputFormat.setBatchWriterOptions(job, writerConfig)
}

/**
 * Output format that turns simple features into mutations and delegates to AccumuloOutputFormat
 */
class GeoMesaOutputFormat extends OutputFormat[Text, SimpleFeature] {

  val delegate = new AccumuloOutputFormat

  override def getRecordWriter(ignored: FileSystem, job: JobConf, name: String, progress: Progressable) = {
  val params = GeoMesaConfigurator.getDataStoreOutParams(job)
  new GeoMesaRecordWriter(params, delegate.getRecordWriter(ignored, job, name, progress))
}

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf) = {
    val params = GeoMesaConfigurator.getDataStoreOutParams(job)
    if (!AccumuloDataStoreFactory.canProcess(params)) {
      throw new IOException("Data store connection parameters are not set")
    }
    delegate.checkOutputSpecs(ignored, job)
  }
}

/**
 * Record writer for GeoMesa SimpleFeatures.
 *
 * Key is ignored. If the feature type for the given feature does not exist yet, it will be created.
 */
class GeoMesaRecordWriter(params: Map[String, String], delegate: RecordWriter[Text, Mutation])
    extends RecordWriter[Text, SimpleFeature] {

  type TableAndMutations = (Text, FeatureToMutations)

  val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

  val sftCache          = scala.collection.mutable.HashSet.empty[String]
  val writerCache       = scala.collection.mutable.Map.empty[String, Seq[TableAndMutations]]
  val encoderCache      = scala.collection.mutable.Map.empty[String, SimpleFeatureSerializer]
  val indexEncoderCache = scala.collection.mutable.Map.empty[String, IndexValueEncoder]

  override def write(key: Text, value: SimpleFeature) = {
    val sft = value.getFeatureType
    val sftName = sft.getTypeName

    // ensure that the type has been created if we haven't seen it before
    if (sftCache.add(sftName)) {
      // this is a no-op if schema is already created, and should be thread-safe from different mappers
      ds.createSchema(value.getFeatureType)
      // short sleep to ensure that feature type is fully written if it is happening in some other thread
      Thread.sleep(5000)
    }

    val writers = writerCache.getOrElseUpdate(sftName, {
      if (sft.getUserData.get(accumulo.index.SFT_INDEX_SCHEMA) == null) {
        // needed for st writer
        sft.getUserData.put(accumulo.index.SFT_INDEX_SCHEMA, ds.getIndexSchemaFmt(sft.getTypeName))
      }
      AccumuloFeatureWriter.getTablesAndWriters(sft, ds).map {
        case (table, writer) => (new Text(table), writer)
      }
    })

    val encoder = encoderCache.getOrElseUpdate(sftName, SimpleFeatureSerializers(sft, ds.getFeatureEncoding(sft)))
    val ive = indexEncoderCache.getOrElseUpdate(sftName, IndexValueEncoder(sft, ds.getGeomesaVersion(sft)))
    val featureToWrite = new FeatureToWrite(value, ds.writeVisibilities, encoder, ive)

    writers.foreach { case (table, featureToMutations) =>
      featureToMutations(featureToWrite).foreach(delegate.write(table, _))
    }
  }

  override def close(reporter: Reporter) = delegate.close(reporter)
}