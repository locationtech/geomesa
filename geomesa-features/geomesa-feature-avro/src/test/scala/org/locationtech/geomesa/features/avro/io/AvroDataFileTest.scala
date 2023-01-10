/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.io

import org.apache.avro.file.DataFileStream
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.features.avro.serialization.SimpleFeatureDatumReader
import org.locationtech.geomesa.features.avro.{AbstractAvroSimpleFeatureTest, io}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.util
import java.util.zip.Deflater

@RunWith(classOf[JUnitRunner])
class AvroDataFileTest extends Specification with AbstractAvroSimpleFeatureTest {

   sequential //because of file delete step

  "AvroDataFile" should {
    "read and write a data file with simple features in it" >> {
      val features = createComplicatedFeatures(50)
      val tmpFile = getTmpFile
      val dfw = new io.AvroDataFileWriter(new FileOutputStream(tmpFile), complexSft)
      try {
        features.foreach(dfw.append)
      } finally {
        dfw.close()
      }

      val readFeatures = getFeatures(tmpFile)
      readFeatures.size mustEqual 50
      readFeatures.map(_.getID) must containTheSameElementsAs(features.map(_.getID))
    }

    "preserve user data" >> {
      val sf = createSimpleFeature
      sf.getUserData.put("key1", "123")
      sf.getUserData.put("key2", "456")
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID("fid1")

      val sf2 = createSimpleFeature
      sf2.getUserData.put("foo", "bar")
      sf2.getUserData.put("baz", "fun")
      sf2.getUserData.put("zzz", null) //try a null
      sf2.getIdentifier.asInstanceOf[FeatureIdImpl].setID("fid2")

      val tmpFile = getTmpFile
      val dfw = new io.AvroDataFileWriter(new FileOutputStream(tmpFile), simpleSft)
      try {
        dfw.append(sf)
        dfw.append(sf2)
      } finally {
        dfw.close()
      }

      val readFeatures = getFeatures(tmpFile)
      readFeatures.size mustEqual 2
      val read1 = readFeatures.find(_.getID == "fid1").head
      val read2 = readFeatures.find(_.getID == "fid2").head
      read1.getUserData.get("key1") mustEqual "123"
      read1.getUserData.get("key2") mustEqual "456"

      read2.getUserData.containsKey("zzz") must beTrue

      import scala.collection.JavaConverters._
      read1.getUserData.asScala.keys must containTheSameElementsAs[AnyRef](sf.getUserData.asScala.keys.toSeq)
      read1.getUserData.asScala.values.toSeq must containTheSameElementsAs[AnyRef](sf.getUserData.asScala.values.toSeq)
      read2.getUserData.asScala.keys must containTheSameElementsAs[AnyRef](sf2.getUserData.asScala.keys.toSeq)
      read2.getUserData.asScala.values.toSeq must containTheSameElementsAs[AnyRef](sf2.getUserData.asScala.values.toSeq)
    }

    "preserve lots of user data" >> {
      val features = createComplicatedFeatures(50)
      val tmpFile = getTmpFile
      val dfw = new io.AvroDataFileWriter(new FileOutputStream(tmpFile), complexSft)
      try {
        features.foreach(dfw.append)
      } finally {
        dfw.close()
      }

      val readFeatures = getFeatures(tmpFile)
      readFeatures.size mustEqual 50
      readFeatures.map(_.getID) must containTheSameElementsAs(features.map(_.getID))
    }

    "write metadata" >> {
      val sf = createSimpleFeature
      sf.getUserData.put("key1", "123")
      sf.getUserData.put("key2", "456")
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID("fid1")

      val tmpFile = getTmpFile
      val dfw = new io.AvroDataFileWriter(new FileOutputStream(tmpFile), simpleSft)
      try {
        dfw.append(sf)
      } finally {
        dfw.close()
      }

      val datumReader = new SimpleFeatureDatumReader()
      val dfs = new DataFileStream[SimpleFeature](new FileInputStream(tmpFile), datumReader)
      dfs.getMetaString(AvroDataFile.SftNameKey) mustEqual simpleSft.getTypeName
      dfs.getMetaString(AvroDataFile.SftSpecKey) mustEqual SimpleFeatureTypes.encodeType(simpleSft)
      dfs.getMetaLong(AvroDataFile.VersionKey) mustEqual 3L
    }

    "support compression" >> {
      import Deflater._
      val features = createComplicatedFeatures(50)
      val uncompressed = getTmpFile
      val compressed = getTmpFile

      Seq((uncompressed, NO_COMPRESSION), (compressed, DEFAULT_COMPRESSION)).foreach { case (file, compression) =>
        val dfw = new io.AvroDataFileWriter(new FileOutputStream(file), complexSft, compression)
        try {
          features.foreach(dfw.append)
        } finally {
          dfw.close()
        }
      }

      compressed.length() must beLessThan(uncompressed.length())

      forall(Seq(uncompressed, compressed)) { file =>
        val readFeatures = getFeatures(file)
        readFeatures.size mustEqual 50
        readFeatures.map(_.getID) must containTheSameElementsAs(features.map(_.getID))
      }
    }

    "serialize byte arrays" >> {
      val features = createComplicatedFeatures(3)
      val tmpFile = getTmpFile
      val dfw = new io.AvroDataFileWriter(new FileOutputStream(tmpFile), complexSft)
      try {
        features.foreach(dfw.append)
      } finally {
        dfw.close()
      }

      val readFeatures = getFeatures(tmpFile)
      readFeatures.size mustEqual 3
      val origBytes = "FOOBARBAZ+12354+\u0000\u0001\u0002\u3434".getBytes(StandardCharsets.UTF_8)
      forall (readFeatures.map(_.getAttribute("f15").asInstanceOf[Array[Byte]])) { arr =>
        arr mustEqual origBytes
      }
    }

    "serialize lists and maps of byte arrays and null stuff everywhere" >> {
      val sft = SimpleFeatureTypes.createType("bytesTest",
        "b1:Bytes,b2:Bytes,m1:Map[String,Bytes],bl:List[Bytes],*geom:Point,dtg:Date")

      val builder = ScalaSimpleFeatureFactory.featureBuilder(sft)
      builder.reset()
      builder.set("b1", "testencoding$^@#$\u0000\u0023".getBytes(StandardCharsets.ISO_8859_1))
      builder.set("b2", null)
      builder.set("m1", Map("a" -> Array(0.toByte, 1.toByte), "b" -> Array(235.toByte)))
      builder.set("bl", List(Array(3.toByte, 4.toByte), Array(10.toByte, 4.toByte)))
      builder.set("geom", WKTUtils.read("POINT(-110 30)"))
      builder.set("dtg", "2012-01-02T05:06:07.000Z")

      val sf = builder.buildFeature("fid")
      val tmpFile = getTmpFile
      val dfw = new io.AvroDataFileWriter(new FileOutputStream(tmpFile), sft)
      try {
        dfw.append(sf)
      } finally {
        dfw.close()
      }
      val readFeatures = getFeatures(tmpFile)
      readFeatures.size mustEqual 1
      val rf = readFeatures.head

      def arrayEquals(a: Any, b: Any): MatchResult[Boolean] = {
        val aBytes = a.asInstanceOf[Array[Byte]]
        val bBytes = b.asInstanceOf[Array[Byte]]
        util.Arrays.equals(aBytes, bBytes) must beTrue
      }
      arrayEquals(rf.getAttribute("b1"), sf.getAttribute("b1"))

      val map1 = rf.getAttribute("m1").asInstanceOf[java.util.Map[String, Array[Byte]]]
      arrayEquals(map1.get("a"), Array(0.toByte, 1.toByte))
      arrayEquals(map1.get("b"), Array(235.toByte))
    }
  }

  step {
    filesCreated.foreach(_.delete)
  }
}

