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

package org.locationtech.geomesa.accumulo.index

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.Value
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.INTERNAL_GEOMESA_VERSION
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexValueEncoderTest extends Specification {

  val defaultSchema = "*geom:Geometry,dtg:Date,s:String,i:Int,d:Double,f:Float,u:UUID,l:List[String]"
  val allSchema = s"*geom:Geometry:$OPT_INDEX_VALUE=true,dtg:Date:$OPT_INDEX_VALUE=true,s:String:$OPT_INDEX_VALUE=true,i:Int:$OPT_INDEX_VALUE=true,d:Double:$OPT_INDEX_VALUE=true,f:Float:$OPT_INDEX_VALUE=true,u:UUID:$OPT_INDEX_VALUE=true,l:List[String]"
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
      IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION).fields must containAllOf(Seq("geom", "dtg"))
    }
    "default to id,geom if no date" in {
      val sft = getSft("*geom:Geometry,foo:String")
      IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION).fields must containAllOf(Seq("geom"))
    }
    "allow custom fields to be set" in {
      val sft = getSft(s"*geom:Geometry:$OPT_INDEX_VALUE=true,dtg:Date:$OPT_INDEX_VALUE=true,s:String,i:Int:$OPT_INDEX_VALUE=true,d:Double,f:Float:$OPT_INDEX_VALUE=true,u:UUID,l:List[String]")
      IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION).fields must containAllOf(Seq("geom", "dtg", "i", "f"))
    }
    "always include id,geom,dtg" in {
      val sft = getSft(s"*geom:Geometry,dtg:Date,s:String,i:Int:$OPT_INDEX_VALUE=true,d:Double,f:Float:$OPT_INDEX_VALUE=true,u:UUID,l:List[String]")
      IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION).fields must containAllOf(Seq("geom", "dtg", "i", "f"))
    }
    "not allow complex types" in {
      val sft = getSft(s"*geom:Geometry:$OPT_INDEX_VALUE=true,dtg:Date:$OPT_INDEX_VALUE=true,l:List[String]:$OPT_INDEX_VALUE=true")
      IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION).fields must containAllOf(Seq("geom", "dtg"))
    }

    "encode and decode id,geom,date" in {
      val sft = getSft()

      // inputs
      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded.getAttributeCount mustEqual(2)
      decoded.getID mustEqual id
      decoded.getAttribute("geom") mustEqual geom
      decoded.getAttribute("dtg") mustEqual dt
    }

    "encode and decode id,geom,date when there is no date" in {
      val sft = getSft()

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, null, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded.getAttribute("dtg") must beNull
      decoded.getAttribute("geom") mustEqual(geom)
      decoded.getID mustEqual(id)
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

      val encoder = IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded.getAttributeCount mustEqual(7)
      decoded.getAttribute("geom") mustEqual geom
      decoded.getID mustEqual id
      decoded.getAttribute("dtg") mustEqual dt
      decoded.getAttribute("d") mustEqual d
      decoded.getAttribute("f") mustEqual f
      decoded.getAttribute("i") mustEqual i
      decoded.getAttribute("s") mustEqual s
      decoded.getAttribute("u") mustEqual u
    }

    "encode and decode null values" in {
      val sft = getSft(allSchema)

      val i: java.lang.Integer = 5
      val d: java.lang.Double = 10d
      val f: java.lang.Float = 1.0f

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, null, null, i, d, f, null, null), id)

      val encoder = IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded.getAttributeCount mustEqual(7)
      decoded.getAttribute("geom") mustEqual geom
      decoded.getID mustEqual id
      decoded.getAttribute("d") mustEqual d
      decoded.getAttribute("f") mustEqual f
      decoded.getAttribute("i") mustEqual i
      decoded.getAttribute("s") must beNull
      decoded.getAttribute("u") must beNull
      decoded.getAttribute("dtg") must beNull
    }

    "maintain backwards compatibility" in {
      val sft = getSft()
      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)
      val encoder = IndexValueEncoder(sft, 0)
      val encoded = _encodeIndexValue(entry)
      val decoded = encoder.decode(encoded.get())
      decoded must not beNull;
      decoded.getAttributeCount mustEqual(2)
      decoded.getAttribute("geom") mustEqual geom
      decoded.getAttribute("dtg") mustEqual dt
      decoded.getID mustEqual id
    }

    "be at least as fast as before" in {
      skipped("for integration")

      val sft = getSft()

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft, INTERNAL_GEOMESA_VERSION)
      val oldEncoder = IndexValueEncoder(sft, 0)

      var totalEncodeNew = 0L
      var totalDecodeNew = 0L

      var totalEncodeOld = 0L
      var totalDecodeOld = 0L

      var totalEncodeOriginal = 0L
      var totalDecodeOriginal = 0L

      // run once to remove any initialization time...
      oldEncoder.decode(oldEncoder.encode(entry))
      encoder.decode(encoder.encode(entry))
      _decodeIndexValue(_encodeIndexValue(entry))

      (0 to 1000000).foreach { _ =>
        val start = System.currentTimeMillis()
        val value = oldEncoder.encode(entry)
        val encode = System.currentTimeMillis()
        oldEncoder.decode(value)
        val decode = System.currentTimeMillis()

        totalEncodeOld += encode - start
        totalDecodeOld += decode - encode
      }

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

        totalEncodeOriginal += encode - start
        totalDecodeOriginal += decode - encode
      }

      println(s"ori $totalEncodeOriginal $totalDecodeOriginal")
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
    val encodedId = entry.getID.getBytes
    val encodedGeom = WKBUtils.write(entry.getDefaultGeometry.asInstanceOf[Geometry])
    // dtg prop is hard-coded here for convenience
    val encodedDtg = Option(entry.getAttribute("dtg").asInstanceOf[Date])
        .map(dtg => ByteBuffer.allocate(8).putLong(dtg.getTime).array()).getOrElse(Array[Byte]())

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
