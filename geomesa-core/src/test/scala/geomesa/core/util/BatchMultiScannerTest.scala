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

package geomesa.core.util

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.TimeZone

import geomesa.core.data._
import geomesa.core.index.IndexSchema
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Value, Range => ARange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.data.DataStoreFinder
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.specs2.execute.Success
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class BatchMultiScannerTest extends Specification {

  val sftName = "bmstest"
  val sft = SimpleFeatureTypes.createType(sftName, s"name:String:index=true,age:String:index=true,idStr:String:index=true,dtg:Date,*geom:Geometry:srid=4326")

  val sdf = new SimpleDateFormat("yyyyMMdd")
  sdf.setTimeZone(TimeZone.getTimeZone("Zulu"))
  val dateToIndex = sdf.parse("20140102")

  val catalogTable = "bmstestcatalog"
  val user = "myuser"
  val pass = "mypassword"
  val instanceName = "bmsTestInst"

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(
      Map(
        "instanceId" -> instanceName,
        "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"       -> user,
        "password"   -> pass,
        "tableName"  -> catalogTable,
        "useMock"    -> "true")
    ).asInstanceOf[AccumuloDataStore]

  val ds = createStore
  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val featureCollection = new DefaultFeatureCollection(sftName, sft)

  for (
    name  <- List("a", "b", "c", "d");
    tuple <- List(1, 2, 3, 4).zip(List(45, 46, 47, 48))
  ) tuple match { case (i, lat) =>
    val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
    sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
    sf.setAttribute("dtg", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
    sf.setAttribute("name", name)
    sf.setAttribute("idStr", sf.getID)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    featureCollection.add(sf)
  }

  fs.addFeatures(featureCollection)

  val nullByte = Array[Byte](0.toByte)
  private val nullString = "<null>"
  private def valOrNull(o: AnyRef) = if (o == null) nullString else o.toString

  case class PutOrDeleteMutation(row: Array[Byte], cf: Text, cq: Text, v: Value)

  def getAttrIdxMutations(feature: SimpleFeature, cf: Text) =
    feature.getFeatureType.getAttributeDescriptors.map { attr =>
      val attrName = attr.getLocalName.getBytes(StandardCharsets.UTF_8)
      val attrValue = valOrNull(feature.getAttribute(attr.getName)).getBytes(StandardCharsets.UTF_8)
      val row = attrName ++ nullByte ++ attrValue
      val value = IndexSchema.encodeIndexValue(feature)
      PutOrDeleteMutation(row, cf, EMPTY_COLQ, value)
    }

  def attrIdxEqualQuery(attr: String, value: String, batchSize: Int): Int = {
    val instance = new MockInstance(instanceName)
    val conn = instance.getConnector(user, new PasswordToken(pass))

    val attrIdxTable = AccumuloDataStore.formatAttrIdxTableName(catalogTable, sft)
    conn.tableOperations.exists(attrIdxTable) must beTrue
    val attrScanner = conn.createScanner(attrIdxTable, new Authorizations())
    attrScanner.setRange(new ARange(new Text(attr.getBytes ++ nullByte ++ value.getBytes)))

    val recordTable = AccumuloDataStore.formatRecordTableName(catalogTable, sft)
    conn.tableOperations().exists(recordTable) must beTrue
    val recordScanner = conn.createBatchScanner(recordTable, new Authorizations(), 5)

    val joinFunction = (kv: java.util.Map.Entry[Key, Value]) => new ARange(kv.getKey.getColumnFamily)
    val bms = new BatchMultiScanner(attrScanner, recordScanner, joinFunction, batchSize)

    val retrieved = bms.iterator.toList
    retrieved.foreach { e =>
      val sf = SimpleFeatureEncoderFactory.defaultEncoder.decode(sft, e.getValue)
      if (value != nullString) {
        sf.getAttribute(attr) mustEqual value
      }
    }

    retrieved.size
  }

  "BatchMultiScanner" should {
    "handle corner cases for attr index queries" in {
      List(1, 2, 3, 4, 5, 6, 8, 15, 16, 17, 200).foreach { batchSize =>
        // test something that exists
        attrIdxEqualQuery("name", "b", batchSize) mustEqual 4

        // test something that doesn't exist!
        attrIdxEqualQuery("name", "doesn't exist", batchSize) mustEqual 0

        // test size of 1
        attrIdxEqualQuery("idStr", "c1", batchSize) mustEqual 1

        // test something that was stored as a null
        attrIdxEqualQuery("age", "43", batchSize) mustEqual 0

        // find nullstring...ugh
        attrIdxEqualQuery("age", "<null>", batchSize) mustEqual 16
      }
      Success()
    }

    "should throw an exception on a bad batch size" in {
      attrIdxEqualQuery("age", "43", 0) must throwA[IllegalArgumentException]
      attrIdxEqualQuery("age", "43", -1) must throwA[IllegalArgumentException]
    }
  }

}
