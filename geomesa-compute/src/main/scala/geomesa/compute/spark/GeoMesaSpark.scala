/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.compute.spark

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import geomesa.core.data.{AccumuloDataStore, AvroFeatureEncoder, FilterToAccumulo}
import geomesa.core.index.IndexSchema
import geomesa.feature.AvroSimpleFeature
import geomesa.utils.geotools.SimpleFeatureTypes
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase, InputConfigurator}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStore, Query}
import org.geotools.factory.CommonFactoryFinder
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

    val indexSchema = IndexSchema(ds.getIndexSchemaFmt(typeName), sft, ds.getFeatureEncoder(typeName))
    val planner = indexSchema.planner

    val filterVisitor = new FilterToAccumulo(sft)
    filterVisitor.visit(query)

    val qp = planner.buildSTIdxQueryPlan(query, filterVisitor, geomesa.core.index.ExplainPrintln)

    ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, ds.connector.whoami(), ds.authToken)
    ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf, ds.connector.getInstance().getInstanceName, ds.connector.getInstance().getZooKeepers)

    InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, ds.getSpatioTemporalIdxTableName(sft))
    InputConfigurator.setRanges(classOf[AccumuloInputFormat], conf, qp.ranges)
    qp.iterators.foreach { is => InputConfigurator.addIterator(classOf[AccumuloInputFormat], conf, is) }

    val rdd = sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])

    rdd.mapPartitions { iter =>
      val sft = SimpleFeatureTypes.createType(typeName, spec)
      val encoder = new AvroFeatureEncoder
      iter.map { case (k: Key, v: Value) => encoder.decode(sft, v) }
    }
  }

  def countByDay(conf: Configuration, sccc: SparkContext, ds: AccumuloDataStore, query: Query, dateField: String = "dtg") = {
    val d = geomesa.compute.spark.GeoMesaSpark.rdd(conf, sccc, ds, query)
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
        val encoder = new AvroFeatureEncoder

        override def write(kryo: Kryo, out: Output, feature: AvroSimpleFeature): Unit = {
          val typeName = feature.getFeatureType.getTypeName
          val len = typeName.length
          out.writeInt(len, true)
          out.write(typeName.getBytes(StandardCharsets.UTF_8))
          val bytes = encoder.encode(feature).get()
          out.writeInt(bytes.length, true)
          out.write(bytes)
        }

        override def read(kry: Kryo, in: Input, clazz: Class[AvroSimpleFeature]): AvroSimpleFeature = {
          val len = in.readInt(true)
          val typeName = new String(in.readBytes(len), StandardCharsets.UTF_8)
          val sft = typeCache.get(typeName)
          val flen = in.readInt(true)
          val bytes = in.readBytes(flen)
          encoder.decode(sft, new ByteArrayInputStream(bytes))
        }
      })
  }
}
