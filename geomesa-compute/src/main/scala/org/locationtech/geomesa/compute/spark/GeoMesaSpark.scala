/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.compute.spark

import java.text.SimpleDateFormat
import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.accumulo.core.client.mapreduce.{RangeInputSplit, AccumuloInputFormat}
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase, InputConfigurator}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, Job}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStore, DataStoreFinder, DefaultTransaction, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.feature._
import org.locationtech.geomesa.feature.kryo.{KryoFeatureSerializer, SimpleFeatureSerializer}
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.{GroupedSplit, GeoMesaInputFormat}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object GeoMesaSpark {

  class GeoSparkInputFormat extends GeoMesaInputFormat
  {
    private def init(conf: Configuration): Unit = if (sft == null) {
      val params = GeoMesaConfigurator.getDataStoreInParams(conf)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
      sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(conf))
      encoding = ds.getFeatureEncoding(sft)
      numShards = IndexSchema.maxShard(ds.getIndexSchemaFmt(sft.getTypeName))
     }

    override def getSplits(context: JobContext) : java.util.List[InputSplit] = {
      init(context.getConfiguration)
      val accumuloSplits = delegate.getSplits(context)
      // try to create 2 mappers per node - account for case where there are less splits than shards
      val desiredSplits = context.getConfiguration.getInt("SplitsDesired", numShards)
      val groupSize = Math.max(desiredSplits * 2, accumuloSplits.length / (desiredSplits * 2))

      // We know each range will only have a single location because of autoAdjustRanges
      val splits = accumuloSplits.groupBy(_.getLocations()(0)).flatMap { case (location, splits) =>
        splits.grouped(groupSize).map { group =>
          val split = new GroupedSplit()
          split.location = location
          split.splits.append(group.map(_.asInstanceOf[RangeInputSplit]): _*)
          split
        }
      }
      splits.toList
    }

  }

  def init(conf: SparkConf, ds: DataStore): SparkConf = {
    val typeOptions = ds.getTypeNames.map { t => (t, SimpleFeatureTypes.encodeType(ds.getSchema(t))) }
    typeOptions.foreach { case (k,v) => System.setProperty(typeProp(k), v) }
    val extraOpts = typeOptions.map { case (k,v) => jOpt(k, v) }.mkString(" ")
    
    conf.set("spark.executor.extraJavaOptions", extraOpts)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getCanonicalName)
  }

  def typeProp(typeName: String) = s"geomesa.types.$typeName"
  def jOpt(typeName: String, spec: String) = s"-D${typeProp(typeName)}=$spec"

  @deprecated("Relies on incidental functional parity across classes, effectively an implicit and unexpressed dependency on non-guaranteed behavior", "Apr2015")
  def rddPartPerExec(conf: Configuration, sc: SparkContext, dsParams: Map[String, String], query: Query, partsPerExec: Int): RDD[SimpleFeature] = {
    val filter = ECQL.toCQL(query.getFilter)
    conf.setInt("SplitsDesired", sc.getExecutorStorageStatus.length * partsPerExec / 2)
    val job = Job.getInstance(conf, "GeoMesa Spark")
    GeoMesaInputFormat.configure(job, dsParams, query.getTypeName, Some(filter)) // this appears to be mutator for job
    sc.newAPIHadoopRDD(job.getConfiguration(), classOf[GeoSparkInputFormat], classOf[Text], classOf[SimpleFeature]).map{ case (t, sf) => sf}
  }

  def rdd(conf: Configuration, sc: SparkContext, ds: AccumuloDataStore, query: Query, useMock: Boolean = false): RDD[SimpleFeature] = {
    val typeName = query.getTypeName
    val sft = ds.getSchema(typeName)
    val spec = SimpleFeatureTypes.encodeType(sft)
    val featureEncoding = ds.getFeatureEncoding(sft)
    val indexSchema = ds.getIndexSchemaFmt(typeName)
    val version = ds.getGeomesaVersion(sft)
    val queryPlanner = new QueryPlanner(sft, featureEncoding, indexSchema, ds, ds.strategyHints(sft), version)

    val qp = new STIdxStrategy().getQueryPlan(query, queryPlanner, ExplainPrintln)

    ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, ds.connector.whoami(), ds.authToken)

    if(useMock) ConfiguratorBase.setMockInstance(classOf[AccumuloInputFormat], conf, ds.connector.getInstance().getInstanceName)
    else ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf, ds.connector.getInstance().getInstanceName, ds.connector.getInstance().getZooKeepers)

    InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, ds.getSpatioTemporalTable(sft))
    InputConfigurator.setRanges(classOf[AccumuloInputFormat], conf, qp.ranges)
    qp.iterators.foreach { is => InputConfigurator.addIterator(classOf[AccumuloInputFormat], conf, is) }

    val rdd = sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])

    rdd.mapPartitions { iter =>
      val sft = SimpleFeatureTypes.createType(typeName, spec)
      val decoder = SimpleFeatureDecoder(sft, featureEncoding)
      iter.map { case (k: Key, v: Value) => decoder.decode(v.get()) }
    }
  }

  /**
   * Writes this RDD to a GeoMesa table.
   * The type must exist in the data store, and all of the features in the RDD must be of this type.
   * @param rdd
   * @param writeDataStoreParams
   * @param writeTypeName
   */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(writeDataStoreParams).asInstanceOf[AccumuloDataStore]
    require(ds.getSchema(writeTypeName) != null, "feature type must exist before calling save.  Call .createSchema on the DataStore before calling .save")

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(writeDataStoreParams).asInstanceOf[AccumuloDataStore]
      val transaction = new DefaultTransaction(UUID.randomUUID().toString)
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, transaction)
      val attrNames = featureWriter.getFeatureType.getAttributeDescriptors.map(_.getLocalName)
      try {
        iter.foreach { case rawFeature =>
          val newFeature = featureWriter.next()
          attrNames.foreach(an => newFeature.setAttribute(an, rawFeature.getAttribute(an)))
          featureWriter.write()
        }
        transaction.commit()
      } finally {
        featureWriter.close()
      }
    }
  }

  def countByDay(conf: Configuration, sccc: SparkContext, ds: AccumuloDataStore, query: Query, dateField: String = "dtg") = {
    val d = rdd(conf, sccc, ds, query)
    val dayAndFeature = d.mapPartitions { iter =>
      val df = new SimpleDateFormat("yyyyMMdd")
      val ff = CommonFactoryFinder.getFilterFactory2
      val exp = ff.property(dateField)
      iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
    }
    val groupedByDay = dayAndFeature.groupBy { case (date, _) => date }
    groupedByDay.map { case (date, iter) => (date, iter.size) }
  }

}

class GeoMesaSparkKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    val serializer = new com.esotericsoftware.kryo.Serializer[SimpleFeature]() {
      val typeCache = CacheBuilder.newBuilder().build(
        new CacheLoader[String, SimpleFeatureType] {
          override def load(key: String): SimpleFeatureType = {
            val spec = System.getProperty(GeoMesaSpark.typeProp(key))
            if (spec == null) throw new IllegalArgumentException(s"Couldn't find property geomesa.types.$key")
            SimpleFeatureTypes.createType(key, spec)
          }
        })

      val serializerCache = CacheBuilder.newBuilder().build(
        new CacheLoader[String, SimpleFeatureSerializer] {
          override def load(key: String): SimpleFeatureSerializer = new SimpleFeatureSerializer(typeCache.get(key))
        })


      override def write(kryo: Kryo, out: Output, feature: SimpleFeature): Unit = {
        val typeName = feature.getFeatureType.getTypeName
        out.writeString(typeName)
        serializerCache.get(typeName).write(kryo, out, feature)
      }

      override def read(kry: Kryo, in: Input, clazz: Class[SimpleFeature]): SimpleFeature = {
        val typeName = in.readString()
        serializerCache.get(typeName).read(kryo, in, clazz)
      }
    }

    KryoFeatureSerializer.setupKryo(kryo, serializer)
  }
}
