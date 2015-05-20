/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

import java.io.{Serializable => JSerializable}
import java.util.{Properties, UUID}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities, Query}
import org.geotools.factory.Hints
import org.joda.time.{DateTime, DateTimeZone}
import org.junit
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class GeoMesaSparkTest extends Specification with Logging {

  sequential

  val testData: Map[String,String] = {
    val dataFile = new Properties
    dataFile.load(getClass.getClassLoader.getResourceAsStream("polygons.properties"))
    dataFile.toMap
  }

  val TEST_TABLE_NAME = "geomesa_spark_test"

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params._
  lazy val dsParams = Map[String, String](
    zookeepersParam.key -> "dummy",
    instanceIdParam.key -> "dummy",
    userParam.key       -> "user",
    passwordParam.key   -> "pass",
    tableNameParam.key  -> TEST_TABLE_NAME,
    mockParam.key       -> "true",
    featureEncParam.key -> "avro")

  lazy val ds: DataStore = {
    val mockInstance = new MockInstance("dummy")
    val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))
    c.tableOperations.create(TEST_TABLE_NAME)
    val splits = (0 to 99).map(s => "%02d".format(s)).map(new Text(_))
    c.tableOperations().addSplits(TEST_TABLE_NAME, new java.util.TreeSet[Text](splits.asJava))

    val dsf = new AccumuloDataStoreFactory

    val ds = dsf.createDataStore(dsParams.mapValues(_.asInstanceOf[JSerializable]).asJava)
    ds
  }

  lazy val spec = "id:Integer,map:Map[String,Integer],dtg:Date,geom:Geometry:srid=4326"

  def createFeatures(ds: DataStore, sft: SimpleFeatureType, encodedFeatures: Array[_<:Array[_]]): Seq[SimpleFeature] = {
    val builder = ScalaSimpleFeatureFactory.featureBuilder(sft)
    val features = encodedFeatures.map {
      e =>
        val f = builder.buildFeature(e(0).toString, e.asInstanceOf[Array[AnyRef]])
        f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        f.getUserData.put(Hints.PROVIDED_FID, e(0).toString)
        f
    }
    features
  }

  def createTypeName() = s"sparktest${UUID.randomUUID().toString}"
  def createSFT(typeName: String) = {
    val t = SimpleFeatureTypes.createType(typeName, spec)
    t.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")
    t
  }

  val randomSeed = 83

  var sc: SparkContext = null

  @junit.After
  def shutdown(): Unit = {
    Option(sc).foreach(_.stop())
  }

  "GeoMesaSpark" should {
    val random = new Random(randomSeed)
    val encodedFeatures = (0 until 150).toArray.map {
      i =>
        Array(
          i.toString,
          Map("a" -> i, "b" -> i * 2, (if (random.nextBoolean()) "c" else "d") -> random.nextInt(10)).asJava,
          new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate,
          "POINT(-77 38)")
    }

    "Read data" in {
      val typeName = s"sparktest${UUID.randomUUID().toString}"
      val sft = createSFT(typeName)

      ds.createSchema(sft)
      ds.getSchema(typeName) should not(beNull)
      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
      val feats = createFeatures(ds, sft, encodedFeatures)
      fs.addFeatures(DataUtilities.collection(feats.asJava))
      fs.getTransaction.commit()

      val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
      GeoMesaSpark.init(conf, ds)
      sc =  new SparkContext(conf) // will get shut down by shutdown method

      val rdd = GeoMesaSpark.rdd(new Configuration(), sc, dsParams, new Query(typeName), useMock = true)

      rdd.count() should equalTo(feats.length)
      feats.map(_.getAttribute("id")) should contain(rdd.take(1).head.getAttribute("id"))
    }

    "Write data" in {
      val typeName = s"sparktest${UUID.randomUUID().toString}"
      val sft = createSFT(typeName)
      ds.createSchema(sft)

      val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
      GeoMesaSpark.init(conf, ds)
      sc =  new SparkContext(conf) // will get shut down by shutdown method
      val feats = createFeatures(ds, sft, encodedFeatures)

      val rdd = sc.makeRDD(feats)

      GeoMesaSpark.save(rdd, dsParams, typeName)

      val coll = ds.getFeatureSource(typeName).getFeatures
      coll.size() should equalTo(encodedFeatures.length)
      feats.map(_.getAttribute("id")) should contain(coll.features().next().getAttribute("id"))
    }
  }
}
