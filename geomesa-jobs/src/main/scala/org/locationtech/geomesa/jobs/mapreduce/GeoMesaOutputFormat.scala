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

package org.locationtech.geomesa.jobs.mapreduce

import java.io.IOException

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, RecordTable, SpatioTemporalTable}
import org.locationtech.geomesa.accumulo.data.{AccumuloFeatureWriter, AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.accumulo.index.{IndexSchema, IndexValueEncoder}
import org.locationtech.geomesa.features.SimpleFeatureSerializers
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object GeoMesaOutputFormat {

  /**
   * Configure the data store you will be writing to.
   */
  def configureDataStore(job: Job, dsParams: Map[String, String]): Unit = {

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
    val conf = job.getConfiguration
    GeoMesaConfigurator.setDataStoreOutParams(conf, dsParams)
    GeoMesaConfigurator.setSerialization(conf)
  }

  /**
   * Configure the batch writer options used by accumulo.
   */
  def configureBatchWriter(job: Job, writerConfig: BatchWriterConfig): Unit =
    AccumuloOutputFormat.setBatchWriterOptions(job, writerConfig)
}

/**
 * Output format that turns simple features into mutations and delegates to AccumuloOutputFormat
 */
class GeoMesaOutputFormat extends OutputFormat[Text, SimpleFeature] {

  val delegate = new AccumuloOutputFormat

  override def getRecordWriter(context: TaskAttemptContext) = {
    val params = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    new GeoMesaRecordWriter(params, delegate.getRecordWriter(context))
  }

  override def checkOutputSpecs(context: JobContext) = {
    val params = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    if (!AccumuloDataStoreFactory.canProcess(params)) {
      throw new IOException("Data store connection parameters are not set")
    }
    delegate.checkOutputSpecs(context)
  }

  override def getOutputCommitter(context: TaskAttemptContext) = delegate.getOutputCommitter(context)
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
  val encoderCache      = scala.collection.mutable.Map.empty[String, org.locationtech.geomesa.features.SimpleFeatureSerializer]
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

  override def close(context: TaskAttemptContext) = delegate.close(context)
}