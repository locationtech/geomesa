/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.io.{BinaryDecoder, DecoderFactory, Encoder, EncoderFactory}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.avro.serde.Version2ASF
import org.locationtech.geomesa.features.serialization.{AbstractWriter, HintKeySerialization}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureWriterTest extends Specification with Mockito with AbstractAvroSimpleFeatureTest {

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
      val fsr = new FeatureSpecificReader(features(0).getFeatureType)
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
