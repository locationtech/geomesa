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
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase, InputConfigurator}
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStore, DataStoreFinder, DefaultTransaction, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index
import org.locationtech.geomesa.accumulo.index.{ExplainNull, QueryPlanner, STIdxStrategy}
import org.locationtech.geomesa.accumulo.stats.QueryStatTransform
import org.locationtech.geomesa.features.SimpleFeatureSerializers
import org.locationtech.geomesa.features.kryo.serialization.SimpleFeatureSerializer
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaInputFormat
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._

object GeoMesaSpark extends Logging {

  def init(conf: SparkConf, ds: DataStore): SparkConf = {
    val typeOptions = ds.getTypeNames.map { t => (t, SimpleFeatureTypes.encodeType(ds.getSchema(t))) }
    typeOptions.foreach { case (k,v) => System.setProperty(typeProp(k), v) }
    val extraOpts = typeOptions.map { case (k,v) => jOpt(k, v) }.mkString(" ")
    
    conf.set("spark.executor.extraJavaOptions", extraOpts)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
  }

  def typeProp(typeName: String) = s"geomesa.types.$typeName"

  def jOpt(typeName: String, spec: String) = s"-D${typeProp(typeName)}=$spec"

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          query: Query,
          numberOfSplits: Option[Int]): RDD[SimpleFeature] = {
    rdd(conf, sc, dsParams, query, false, numberOfSplits)
  }

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          query: Query,
          useMock: Boolean = false,
          numberOfSplits: Option[Int] = None): RDD[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    val typeName = query.getTypeName
    val sft = ds.getSchema(typeName)
    val spec = SimpleFeatureTypes.encodeType(sft)
    val featureEncoding = ds.getFeatureEncoding(sft)
    val indexSchema = ds.getIndexSchemaFmt(typeName)
    val version = ds.getGeomesaVersion(sft)
    val queryPlanner = new QueryPlanner(sft, featureEncoding, indexSchema, ds, ds.strategyHints(sft), version)

    val qps = new STIdxStrategy().getQueryPlans(query, queryPlanner, ExplainNull)
    if (qps.length > 1) {
      logger.error("The query being executed requires multiple scans, which is not currently " +
          "supported by geomesa. Your result set will be partially incomplete. This is most likely due to " +
          s"an OR clause in your query. Query: ${QueryStatTransform.filterToString(query.getFilter)}")
    }
    val qp = qps.head

    ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, ds.connector.whoami(), ds.authToken)

    if (useMock){
      ConfiguratorBase.setMockInstance(classOf[AccumuloInputFormat],
        conf,
        ds.connector.getInstance().getInstanceName)
    } else {
      ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat],
        conf,
        ds.connector.getInstance().getInstanceName,
        ds.connector.getInstance().getZooKeepers)
    }
    InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, ds.getSpatioTemporalTable(sft))
    InputConfigurator.setRanges(classOf[AccumuloInputFormat], conf, qp.ranges)
    qp.iterators.foreach { is => InputConfigurator.addIterator(classOf[AccumuloInputFormat], conf, is)}

    if (!qp.columnFamilies.isEmpty) {
      InputConfigurator.fetchColumns(classOf[AccumuloInputFormat],
        conf,
        qp.columnFamilies.map(cf => new AccPair[Text, Text](cf, null)))
    }

    if (numberOfSplits.isDefined) {
      GeoMesaConfigurator.setDesiredSplits(conf,
        numberOfSplits.get * sc.getExecutorStorageStatus.length)
      InputConfigurator.setAutoAdjustRanges(classOf[AccumuloInputFormat], conf, false)
      InputConfigurator.setAutoAdjustRanges(classOf[GeoMesaInputFormat], conf, false)
    }
    GeoMesaConfigurator.setSerialization(conf)
    GeoMesaConfigurator.setDataStoreInParams(conf, dsParams)
    GeoMesaConfigurator.setFeatureType(conf, typeName)
    if (query.getFilter != Filter.INCLUDE) {
      GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(query.getFilter))
    }

    index.getTransformSchema(query).foreach(GeoMesaConfigurator.setTransformSchema(conf, _))

    sc.newAPIHadoopRDD(conf, classOf[GeoMesaInputFormat], classOf[Text], classOf[SimpleFeature]).map(U => U._2)

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

  def countByDay(conf: Configuration, sccc: SparkContext, dsParams: Map[String, String], query: Query, dateField: String = "dtg") = {
    val d = rdd(conf, sccc, dsParams, query)
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

    kryo.setReferences(false)
    SimpleFeatureSerializers.simpleFeatureImpls.foreach(kryo.register(_, serializer, kryo.getNextRegistrationId))
  }
}
