/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.avro

import java.io.{FileInputStream, FileOutputStream}
import java.util.zip.Deflater

import org.apache.avro.file.DataFileStream
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroDataFileTest extends Specification with AbstractAvroSimpleFeatureTest {

   sequential //because of file delete step

  "AvroDataFile" should {
    "read and write a data file with simple features in it" >> {
      val features = createComplicatedFeatures(50)
      val tmpFile = getTmpFile
      val dfw = new AvroDataFileWriter(new FileOutputStream(tmpFile), complexSft)
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
      val dfw = new AvroDataFileWriter(new FileOutputStream(tmpFile), simpleSft)
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

      import scala.collection.JavaConversions._
      read1.getUserData.keys must containTheSameElementsAs[AnyRef](sf.getUserData.keys.toSeq)
      read1.getUserData.values.toSeq must containTheSameElementsAs[AnyRef](sf.getUserData.values.toSeq)
      read2.getUserData.keys must containTheSameElementsAs[AnyRef](sf2.getUserData.keys.toSeq)
      read2.getUserData.values.toSeq must containTheSameElementsAs[AnyRef](sf2.getUserData.values.toSeq)
    }

    "preserve lots of user data" >> {
      val features = createComplicatedFeatures(50)
      features.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))
      val tmpFile = getTmpFile
      val dfw = new AvroDataFileWriter(new FileOutputStream(tmpFile), complexSft)
      try {
        features.foreach(dfw.append)
      } finally {
        dfw.close()
      }

      val readFeatures = getFeatures(tmpFile)
      readFeatures.size mustEqual 50
      readFeatures.map(_.getID) must containTheSameElementsAs(features.map(_.getID))
      readFeatures.forall { sf =>
        sf.getUserData.get(Hints.USE_PROVIDED_FID) mustEqual java.lang.Boolean.TRUE
      }
    }

    "write metadata" >> {
      val sf = createSimpleFeature
      sf.getUserData.put("key1", "123")
      sf.getUserData.put("key2", "456")
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID("fid1")

      val tmpFile = getTmpFile
      val dfw = new AvroDataFileWriter(new FileOutputStream(tmpFile), simpleSft)
      try {
        dfw.append(sf)
      } finally {
        dfw.close()
      }

      val datumReader = new FeatureSpecificReader(null, null, SerializationOptions.withUserData)
      val dfs = new DataFileStream[AvroSimpleFeature](new FileInputStream(tmpFile), datumReader)
      dfs.getMetaString(AvroDataFile.SftNameKey) mustEqual simpleSft.getTypeName
      dfs.getMetaString(AvroDataFile.SftSpecKey) mustEqual SimpleFeatureTypes.encodeType(simpleSft)
      dfs.getMetaLong(AvroDataFile.VersionKey) mustEqual 1L
    }

    "support compression" >> {
      import Deflater._
      val features = createComplicatedFeatures(50)
      val uncompressed = getTmpFile
      val compressed = getTmpFile

      Seq((uncompressed, NO_COMPRESSION), (compressed, DEFAULT_COMPRESSION)).foreach { case (file, compression) =>
        val dfw = new AvroDataFileWriter(new FileOutputStream(file), complexSft, compression)
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
  }

  step {
    filesCreated.foreach(_.delete)
  }
}

