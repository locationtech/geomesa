/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import org.apache.avro.io.{BinaryDecoder, DecoderFactory, Encoder, EncoderFactory}
import org.geotools.api.feature.simple.SimpleFeature
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.avro.AbstractAvroSimpleFeatureTest
import org.locationtech.geomesa.features.avro.serde.Version2ASF
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

@RunWith(classOf[JUnitRunner])
class SimpleFeatureDatumWriterTest extends Specification with Mockito with AbstractAvroSimpleFeatureTest {

  import scala.collection.JavaConverters._

  sequential

  "SimpleFeatureDatumWriter2" should {

    "correctly serialize all the datatypes provided in AvroSimpleFeature" in {
      val features = createComplicatedFeatures(10)

      val oldBaos = new ByteArrayOutputStream()
      def serializeOld(sf: SimpleFeature) = {
        oldBaos.reset()
        Version2ASF(sf).write(oldBaos)
        oldBaos.toByteArray
      }

      val afw = new SimpleFeatureDatumWriter(features(0).getFeatureType)
      val newBaos = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().directBinaryEncoder(newBaos, null)
      def serializeNew(sf: SimpleFeature) = {
        newBaos.reset()
        afw.write(sf, encoder)
        encoder.flush()
        newBaos.toByteArray
      }

      var decoder: BinaryDecoder = null
      val fsr = SimpleFeatureDatumReader(afw.getSchema, features(0).getFeatureType)
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
        n.getAttributeCount mustEqual 16
        n.getAttributes.asScala.dropRight(1) mustEqual o.getAttributes.asScala.dropRight(1)
        util.Arrays.equals(n.getAttributes.asScala.last.asInstanceOf[Array[Byte]], o.getAttributes.asScala.last.asInstanceOf[Array[Byte]]) must beTrue
      }

      newFeatures.zip(features).foreach { case (n, o) =>
        n.getID mustEqual o.getID
        n.getAttributeCount mustEqual o.getAttributeCount
        n.getAttributeCount mustEqual 16
        n.getAttributes.asScala.dropRight(1) mustEqual o.getAttributes.asScala.dropRight(1)
        util.Arrays.equals(n.getAttributes.asScala.last.asInstanceOf[Array[Byte]], o.getAttributes.asScala.last.asInstanceOf[Array[Byte]]) must beTrue
      }

      success
    }

    "serialize user data when requested" >> {
      import org.locationtech.geomesa.security._
      val sf = createSimpleFeature

      val vis = "private&groupA"
      sf.visibility = vis

      val userData = sf.getUserData
      userData.put(java.lang.Integer.valueOf(55), null)
      userData.put(null, "null key")

      val afw = new SimpleFeatureDatumWriter(sf.getType, SerializationOptions.withUserData)
      val encoder = mock[Encoder]

      afw.write(sf, encoder)

      there was one(encoder).writeArrayStart()

      there was one(encoder).setItemCount(3)
      there was 3.times(encoder).startItem()

      // 1 key and 1 value are null
      there was 2.times(encoder).writeNull()

      // visibility data
      there was one(encoder).writeString(SecurityUtils.FEATURE_VISIBILITY)
      there was one(encoder).writeString(vis)

      // key = 55, value = null
      there was one(encoder).writeInt(55)

      // key = null, value = "null key"
      there was one(encoder).writeString("null key")

      there was one(encoder).writeArrayEnd()
    }

    "use unmangled names when requested" >> {
      import org.locationtech.geomesa.security._
      val sf = createSimpleFeature

      val vis = "private&groupA"
      sf.visibility = vis

      val userData = sf.getUserData
      userData.put(java.lang.Integer.valueOf(5), null)
      userData.put(null, "null key")

      val afw = new SimpleFeatureDatumWriter(sf.getType, SerializationOptions.withUserData)
      val encoder = mock[Encoder]

      afw.write(sf, encoder)

      there was one(encoder).writeArrayStart()
    }
  }

}
