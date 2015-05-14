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

package org.locationtech.geomesa.features.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.text.SimpleDateFormat
import java.util.UUID

import com.vividsolutions.jts.geom.{Point, Polygon}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, Encoder, EncoderFactory}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.avro.serde.Version2ASF
import org.locationtech.geomesa.features.serialization.{AbstractWriter, HintKeySerialization}
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ListBuffer
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureWriterTest extends Specification with Mockito {

  def createComplicatedFeatures(numFeatures : Int) : List[Version2ASF] = {
    val geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,"+
      "f8:Polygon:srid=4326,f9:Long,f10:String,f11:Integer,f12:Date,f13:Geometry,f14:UUID"
    val sft = SimpleFeatureTypes.createType("test", geoSchema)
    val r = new Random()
    r.setSeed(0)

    val list = new ListBuffer[Version2ASF]
    for(i <- 0 until numFeatures){
      val fid = new FeatureIdImpl(r.nextString(5))
      val sf = new Version2ASF(fid, sft)

      sf.setAttribute("f0", r.nextString(10).asInstanceOf[Object])
      sf.setAttribute("f1", r.nextInt().asInstanceOf[Object])
      sf.setAttribute("f2", r.nextDouble().asInstanceOf[Object])
      sf.setAttribute("f3", r.nextFloat().asInstanceOf[Object])
      sf.setAttribute("f4", r.nextBoolean().asInstanceOf[Object])
      sf.setAttribute("f5", UUID.fromString("12345678-1234-1234-1234-123456789012"))
      sf.setAttribute("f6", new SimpleDateFormat("yyyyMMdd").parse("20140102"))
      sf.setAttribute("f7", GeohashUtils.wkt2geom("POINT(45.0 49.0)").asInstanceOf[Point])
      sf.setAttribute("f8", GeohashUtils.wkt2geom("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))").asInstanceOf[Polygon])
      sf.setAttribute("f9", r.nextLong().asInstanceOf[Object])

      // test nulls on a few data types
      "f10,f11,f12,f13,f14".split(",").foreach { id =>
        sf.setAttribute(id, null.asInstanceOf[Object])
      }

      list += sf
    }
    list.toList
  }

  def createSimpleFeature: SimpleFeature = {
    val sft = SimpleFeatureTypes.createType("AvroSimpleFeatureWriterTest", "name:String,*geom:Point,dtg:Date")

    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    builder.reset()
    builder.set("name", "test_feature")
    builder.set("geom", WKTUtils.read("POINT(-110 30)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")

    val sf = builder.buildFeature("fid")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  "AvroSimpleFeatureWriter2" should {

    "correctly serialize features compared to old integrated AvroSimpleFeature write() method" in {
      val sft = SimpleFeatureTypes.createType("testType", "a:Integer,b:Date,*geom:Point:srid=4326")

      val f = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)
      f.setAttribute(0,"1")
      f.setAttribute(1,"2013-01-02T00:00:00.000Z")
      f.setAttribute(2,"POINT(45.0 49.0)")

      val oldBaos = new ByteArrayOutputStream()
      Version2ASF(f).write(oldBaos)
      val oldBytes = oldBaos.toByteArray

      val afw = new AvroSimpleFeatureWriter(sft)
      val newBaos = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().directBinaryEncoder(newBaos, null)
      afw.write(f, encoder)
      encoder.flush()
      val newBytes = newBaos.toByteArray

      newBytes mustEqual oldBytes
    }

    "correctly serialize all the datatypes provided in AvroSimpleFeature" in {
      val features = createComplicatedFeatures(10)

      val oldBaos = new ByteArrayOutputStream()
      def serializeOld(sf: SimpleFeature) = {
        oldBaos.reset()
        Version2ASF(sf).write(oldBaos)
        oldBaos.toByteArray
      }

      val afw = new AvroSimpleFeatureWriter(features(0).getFeatureType)
      val newBaos = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().directBinaryEncoder(newBaos, null)
      def serializeNew(sf: SimpleFeature) = {
        newBaos.reset()
        afw.write(sf, encoder)
        encoder.flush()
        newBaos.toByteArray
      }

      var decoder: BinaryDecoder = null
      val fsr = FeatureSpecificReader(features(0).getFeatureType)
      def convert(bytes: Array[Byte]) = {
        val bais = new ByteArrayInputStream(bytes)
        decoder = DecoderFactory.get().directBinaryDecoder(bais, decoder)
        fsr.read(null, decoder)
      }

      val oldFeatures = features.map(serializeOld).map(convert)
      val newFeatures = features.map(serializeNew).map(convert)

      newFeatures.zip(oldFeatures).foreach { case (n, o) =>
        n.getID mustEqual o.getID
        n.getAttributeCount mustEqual o.getAttributeCount
        n.getAttributeCount mustEqual 15
        n.getAttributes mustEqual o.getAttributes
      }

      newFeatures.zip(features).foreach { case (n, o) =>
        n.getID mustEqual o.getID
        n.getAttributeCount mustEqual o.getAttributeCount
        n.getAttributeCount mustEqual 15
        n.getAttributes mustEqual o.getAttributes
      }

      success
    }

    "serialize user data when requested" >> {
      import org.locationtech.geomesa.security._
      val sf = createSimpleFeature

      val vis = "test&usa"
      sf.visibility = vis

      val userData = sf.getUserData
      userData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      userData.put(java.lang.Integer.valueOf(5), null)
      userData.put(null, "null key")

      val afw = new AvroSimpleFeatureWriter(sf.getType, SerializationOptions.withUserData)
      val encoder = mock[Encoder]

      afw.write(sf, encoder)

      there was one(encoder).writeArrayStart()

      there was one(encoder).setItemCount(4)
      there was 4.times(encoder).startItem()

      // 1 key  and 2 values have type String
      there was three(encoder).writeString("java.lang.String")

      // 1 key  and 0 values have type Hints.Key
      there was one(encoder).writeString(classOf[Hints.Key].getName)

      // 0 keys and 1 value  have type Boolean
      there was one(encoder).writeString("java.lang.Boolean")

      // 1 key  and 0 values have type Integer
      there was one(encoder).writeString("java.lang.Boolean")

      // 1 key  and 1 value  are null
      there was two(encoder).writeString(AbstractWriter.NULL_MARKER_STR)

      // visibility data
      there was one(encoder).writeString(SecurityUtils.FEATURE_VISIBILITY)
      there was one(encoder).writeString(vis)

      // hint data
      there was one(encoder).writeString(HintKeySerialization.keyToId(Hints.USE_PROVIDED_FID))
      there was one(encoder).writeBoolean(true)

      // key = 5, value = null
      there was one(encoder).writeInt(5)

      // key = null, value = "null key"
      there was one(encoder).writeString("null key")

      there was one(encoder).writeArrayEnd()
    }
  }

}
