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

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase, InputConfigurator}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStore, Query}
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.{IndexSchema, STIdxStrategy}
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object GeoMesaSpark {

  def init(conf: SparkConf, ds: DataStore): SparkConf = {
    val typeOptions = ds.getTypeNames.map { typeName =>
      val spec = SimpleFeatureTypes.encodeType(ds.getSchema(typeName))
      s"-Dgeomesa.types.$typeName=$spec"
    }.mkString(" ")
    conf.set("spark.executor.extraJavaOptions", typeOptions)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[KryoAvroSimpleFeatureBridge].getCanonicalName)
  }

  def rdd(conf: Configuration, sc: SparkContext, ds: AccumuloDataStore, query: Query): RDD[SimpleFeature] = {
    val typeName = query.getTypeName
    val sft = ds.getSchema(typeName)
    val spec = SimpleFeatureTypes.encodeType(sft)
    val encoder = SimpleFeatureEncoder(sft, ds.getFeatureEncoding(sft))
    val decoder = SimpleFeatureDecoder(sft, ds.getFeatureEncoding(sft))

    val indexSchema = IndexSchema(ds.getIndexSchemaFmt(typeName), sft, encoder)

    val planner = new STIdxStrategy
    val qp = planner.buildSTIdxQueryPlan(query, indexSchema.planner, sft, org.locationtech.geomesa.core.index.ExplainPrintln)

    ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, ds.connector.whoami(), ds.authToken)
    ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf, ds.connector.getInstance().getInstanceName, ds.connector.getInstance().getZooKeepers)

    InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, ds.getSpatioTemporalIdxTableName(sft))
    InputConfigurator.setRanges(classOf[AccumuloInputFormat], conf, qp.ranges)
    InputConfigurator.setOfflineTableScan(classOf[AccumuloInputFormat], conf, true)
    qp.iterators.foreach { is => InputConfigurator.addIterator(classOf[AccumuloInputFormat], conf, is) }

    val rdd = sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])

    rdd.mapPartitions { iter =>
      val sft = SimpleFeatureTypes.createType(typeName, spec)
      val encoder = new AvroFeatureEncoder(sft)
      iter.map { case (k: Key, v: Value) => decoder.decode(v) }
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

class KryoAvroSimpleFeatureBridge extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[AvroSimpleFeature],
      new com.esotericsoftware.kryo.Serializer[AvroSimpleFeature]() {
        val typeCache = CacheBuilder.newBuilder().build(
          new CacheLoader[String, SimpleFeatureType] {
            override def load(key: String): SimpleFeatureType = {
              val spec = System.getProperty(s"geomesa.types.$key")
              SimpleFeatureTypes.createType(key, spec)
            }
          })

        val encoderCache = CacheBuilder.newBuilder().build(
          new CacheLoader[String, AvroFeatureEncoder] {
            override def load(key: String): AvroFeatureEncoder = new AvroFeatureEncoder(typeCache.get(key))
          })

        val decoderCache = CacheBuilder.newBuilder().build(
          new CacheLoader[String, AvroFeatureDecoder] {
            override def load(key: String): AvroFeatureDecoder = new AvroFeatureDecoder(typeCache.get(key))
          })

        override def write(kryo: Kryo, out: Output, feature: AvroSimpleFeature): Unit = {
          val typeName = feature.getFeatureType.getTypeName
          val len = typeName.length
          out.writeInt(len, true)
          out.write(typeName.getBytes(StandardCharsets.UTF_8))
          val bytes = encoderCache.get(typeName).encode(feature)
          out.writeInt(bytes.length, true)
          out.write(bytes)
        }

        override def read(kry: Kryo, in: Input, clazz: Class[AvroSimpleFeature]): AvroSimpleFeature = {
          val len = in.readInt(true)
          val typeName = new String(in.readBytes(len), StandardCharsets.UTF_8)
          val sft = typeCache.get(typeName)
          val flen = in.readInt(true)
          val bytes = in.readBytes(flen)
          decoderCache.get(typeName).decode(bytes)
        }
      })
  }
}
