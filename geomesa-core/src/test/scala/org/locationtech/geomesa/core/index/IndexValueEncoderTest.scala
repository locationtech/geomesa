/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.index

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.Value
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexValueEncoderTest extends Specification {

  val defaultSchema = "*geom:Geometry,dtg:Date,s:String,i:Int,d:Double,f:Float,u:UUID,l:List[String]"
  val allSchema = "*geom:Geometry:stidx=true,dtg:Date:stidx=true,s:String:stidx=true,i:Int:stidx=true,d:Double:stidx=true,f:Float:stidx=true,u:UUID:stidx=true,l:List[String]"
  val id = "Feature0123456789"
  val geom = WKTUtils.read("POINT (-78.495356 38.075215)")
  val dt = new DateTime().toDate

  // b/c the IndexValueEncoder caches feature types, we need to change the sft name for each test
  val index = new AtomicInteger(0)
  def getSft(schema: String = defaultSchema) =
    SimpleFeatureTypes.createType("IndexValueEncoderTest" + index.getAndIncrement, schema)

  "IndexValueEncoder" should {
    "default to id,geom,date" in {
      val sft = getSft()
      IndexValueEncoder(sft).fields must containAllOf(Seq("id", "geom", "dtg"))
    }
    "default to id,geom if no date" in {
      val sft = getSft("*geom:Geometry,foo:String")
      IndexValueEncoder(sft).fields must containAllOf(Seq("id", "geom"))
    }
    "allow custom fields to be set" in {
      val sft = getSft("*geom:Geometry:stidx=true,dtg:Date:stidx=true,s:String,i:Int:stidx=true,d:Double,f:Float:stidx=true,u:UUID,l:List[String]")
      IndexValueEncoder(sft).fields must containAllOf(Seq("id", "geom", "dtg", "i", "f"))
    }
    "always include id,geom,dtg" in {
      val sft = getSft("*geom:Geometry,dtg:Date,s:String,i:Int:stidx=true,d:Double,f:Float:stidx=true,u:UUID,l:List[String]")
      IndexValueEncoder(sft).fields must containAllOf(Seq("id", "geom", "dtg", "i", "f"))
    }
    "not allow complex types" in {
      val sft = getSft("*geom:Geometry:stidx=true,dtg:Date:stidx=true,l:List[String]:stidx=true")
      IndexValueEncoder(sft).fields must containAllOf(Seq("id", "geom", "dtg"))
    }

    "encode and decode id,geom,date" in {
      val sft = getSft()

      // inputs
      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded.attributes must haveSize(3)
      decoded.id mustEqual id
      decoded.geom mustEqual geom
      decoded.date mustEqual Some(dt)
    }

    "encode and decode id,geom,date when there is no date" in {
      val sft = getSft()

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, null, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded.attributes must haveSize(3)
      decoded.date must beNone
      decoded.geom mustEqual(geom)
      decoded.id mustEqual(id)
    }

    "encode and decode custom fields" in {
      val sft = getSft(allSchema)

      val s = "test"
      val i: java.lang.Integer = 5
      val d: java.lang.Double = 10d
      val f: java.lang.Float = 1.0f
      val u = UUID.randomUUID()

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, s, i, d, f, u, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded.attributes must haveSize(8)
      decoded.geom mustEqual geom
      decoded.id mustEqual id
      decoded.date mustEqual Some(dt)
      decoded.attributes("d") mustEqual d
      decoded.attributes("f") mustEqual f
      decoded.attributes("i") mustEqual i
      decoded.attributes("s") mustEqual s
      decoded.attributes("u") mustEqual u
    }

    "encode and decode null values" in {
      val sft = getSft(allSchema)

      val i: java.lang.Integer = 5
      val d: java.lang.Double = 10d
      val f: java.lang.Float = 1.0f

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, null, null, i, d, f, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded.attributes must haveSize(8)
      decoded.geom mustEqual geom
      decoded.id mustEqual id
      decoded.attributes("d") mustEqual d
      decoded.attributes("f") mustEqual f
      decoded.attributes("i") mustEqual i
      decoded.attributes("s") must beNull
      decoded.attributes("u") must beNull
      decoded.date must beNone
    }

    "maintain backwards compatibility" in {
      val sft = getSft()
      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)
      val encoder = IndexValueEncoder(sft)
      val encoded = _encodeIndexValue(entry)
      val decoded = encoder.decode(encoded.get())
      decoded must not beNull;
      decoded.attributes must haveSize(3)
      decoded.geom mustEqual geom
      decoded.date mustEqual Some(dt)
      decoded.id mustEqual id
    }

    "be at least as fast as before" in {
      skipped("for integration")

      val sft = getSft()

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft)

      var totalEncodeNew = 0L
      var totalDecodeNew = 0L

      var totalEncodeOld = 0L
      var totalDecodeOld = 0L

      // run once to remove any initialization time...
      _encodeIndexValue(entry)
      encoder.encode(entry)

      (0 to 1000000).foreach { _ =>
        val start = System.currentTimeMillis()
        val value = encoder.encode(entry)
        val encode = System.currentTimeMillis()
        encoder.decode(value)
        val decode = System.currentTimeMillis()

        totalEncodeNew += encode - start
        totalDecodeNew += decode - encode
      }

      (0 to 1000000).foreach { _ =>
        val start = System.currentTimeMillis()
        val value = _encodeIndexValue(entry)
        val encode = System.currentTimeMillis()
        _decodeIndexValue(value)
        val decode = System.currentTimeMillis()

        totalEncodeOld += encode - start
        totalDecodeOld += decode - encode
      }

      println(s"old $totalEncodeOld $totalDecodeOld")
      println(s"new $totalEncodeNew $totalDecodeNew")
      println
      success
    }
  }

  // FOLLOWING METHODS ARE THE OLD ONES FROM IndexEntry

  // the index value consists of the feature's:
  // 1.  ID
  // 2.  WKB-encoded geometry
  // 3.  start-date/time
  def _encodeIndexValue(entry: SimpleFeature): Value = {
    import IndexEntry._
    val encodedId = entry.sid.getBytes
    val encodedGeom = WKBUtils.write(entry.geometry)
    val encodedDtg = entry.dt.map(dtg => ByteBuffer.allocate(8).putLong(dtg.getMillis).array()).getOrElse(Array[Byte]())

    new Value(
      ByteBuffer.allocate(4).putInt(encodedId.length).array() ++ encodedId ++
          ByteBuffer.allocate(4).putInt(encodedGeom.length).array() ++ encodedGeom ++
          encodedDtg)
  }

  def _decodeIndexValue(v: Value): _DecodedIndexValue = {
    val buf = v.get()
    val idLength = ByteBuffer.wrap(buf, 0, 4).getInt
    val (idPortion, geomDatePortion) = buf.drop(4).splitAt(idLength)
    val id = new String(idPortion)
    val geomLength = ByteBuffer.wrap(geomDatePortion, 0, 4).getInt
    if(geomLength < (geomDatePortion.length - 4)) {
      val (l,r) = geomDatePortion.drop(4).splitAt(geomLength)
      _DecodedIndexValue(id, WKBUtils.read(l), Some(ByteBuffer.wrap(r).getLong))
    } else {
      _DecodedIndexValue(id, WKBUtils.read(geomDatePortion.drop(4)), None)
    }
  }

  case class _DecodedIndexValue(id: String, geom: Geometry, dtgMillis: Option[Long])
}
