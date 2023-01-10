/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.commons.io.IOUtils
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.DataUtilities
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.text.SimpleDateFormat
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SimpleFeatureDatumReaderTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  def createTypeWithGeo: SimpleFeature = {
    val sft = SimpleFeatureTypes.createType("test","f0:Point,f1:Polygon,f2:LineString")
    val sf = new ScalaSimpleFeature(sft, "fakeid")

    sf.setAttribute("f0", WKTUtils.read("POINT(45.0 49.0)"))
    sf.setAttribute("f1", WKTUtils.read("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))"))
    sf.setAttribute("f2", WKTUtils.read("LINESTRING(47.28515625 25.576171875, 48 26, 49 27)"))

    sf
  }

  def writeAvroFile(sfList: Seq[SimpleFeature]) : File = {
    val f = File.createTempFile("avro", ".tmp")
    f.deleteOnExit()
    val fos = new FileOutputStream(f)
    val writer = new SimpleFeatureDatumWriter(sfList.head.getFeatureType)

    // Use a regular binary encoder for buffered file writes
    val encoder = EncoderFactory.get().binaryEncoder(fos, null)
    sfList.foreach { sf =>
      writer.write(sf, encoder)
    }
    encoder.flush()
    fos.close()
    f
  }

  def readAvroFile(sft: SimpleFeatureType, f: File): List[SimpleFeature] = {
    val fis = new FileInputStream(f)
    val decoder = DecoderFactory.get().binaryDecoder(fis, null)
    val reader = new SimpleFeatureDatumReader()
    reader.setSchema(AvroSerialization(sft, Set.empty).schema)
    reader.setFeatureType(sft)

    val sfList = new ListBuffer[SimpleFeature]()

    while (!decoder.isEnd) {
      sfList += reader.read(null, decoder)
    }

    fis.close()
    sfList.toList
  }

  def writePipeFile(sfList: Seq[SimpleFeature]) : File = {
    val f = File.createTempFile("pipe", ".tmp")
    f.deleteOnExit()
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), UTF_8))
    sfList.foreach { f =>
      writer.write(DataUtilities.encodeFeature(f, true))
      writer.newLine()
    }
    writer.close()
    f
  }

  def readPipeFile(f: File, sft: SimpleFeatureType) : List[SimpleFeature] = {
    val sfList = ListBuffer[SimpleFeature]()
    WithClose(new FileInputStream(f)) { is =>
      IOUtils.lineIterator(is, StandardCharsets.UTF_8).asScala.foreach { line =>
        sfList += ScalaSimpleFeature.copy(DataUtilities.createFeature(sft, line)) // use ScalaSimpleFeature for equality comparison
      }
    }
    sfList.toList
  }

  def buildStringSchema(numFields: Int) : String =
    Seq.tabulate(numFields)(i => f"f$i%d:String").mkString(",")

  def randomString(fieldId: Int, len: Int) : String = Seq.fill(len)(fieldId).mkString

  def createStringFeatures(sft:SimpleFeatureType, size: Int, id: String): SimpleFeature = {
    val sf = new ScalaSimpleFeature(sft, id)
    for(i <- 0 until size) {
      sf.setAttribute(i, randomString(i, 8))
    }
    sf
  }

  "SimpleFeatureDatumReader" should {
    "read geometry types" in {
      val orig = createTypeWithGeo
      val f = writeAvroFile(List(orig))
      val fsrList = readAvroFile(orig.getType, f)

      fsrList must haveSize(1)
      val sf = fsrList.head
      sf.getAttributeCount mustEqual 3
      sf.getAttributeCount mustEqual orig.getAttributeCount

      sf.getAttribute("f0") mustEqual orig.getAttribute("f0")
      sf.getAttribute("f1") mustEqual orig.getAttribute("f1")
      sf.getAttribute("f2") mustEqual orig.getAttribute("f2")
    }

    "read strings" in {
      val numFields = 60
      val numRecords = 100
      val geoSchema = buildStringSchema(numFields)

      val sft = SimpleFeatureTypes.createType("test", geoSchema)
      val features = Seq.tabulate(numRecords)(i => createStringFeatures(sft, numFields, i.toString))

      val avroFile = writeAvroFile(features)
      val pipeFile = writePipeFile(features)

      val avroList = readAvroFile(sft, avroFile)
      val pipeList = readPipeFile(pipeFile, sft)

      foreach(Seq(avroList, pipeList))(_ must haveSize(features.length))

      foreach(features.zip(avroList).zip(pipeList)) { case ((feature, avro), pipe) =>
        avro mustEqual feature
        pipe mustEqual feature
      }
    }

    "read different types of fields" in {
      val numRecords = 1
      val geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,f8:Polygon:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", geoSchema)
      val r = new Random()
      r.setSeed(0)

      val features = Seq.fill(numRecords) {
        val sf = new ScalaSimpleFeature(sft, r.nextString(5))

        sf.setAttribute("f0", r.nextString(10).asInstanceOf[Object])
        sf.setAttribute("f1", r.nextInt().asInstanceOf[Object])
        sf.setAttribute("f2", r.nextDouble().asInstanceOf[Object])
        sf.setAttribute("f3", r.nextFloat().asInstanceOf[Object])
        sf.setAttribute("f4", r.nextBoolean().asInstanceOf[Object])
        sf.setAttribute("f5", UUID.fromString("12345678-1234-1234-1234-123456789012"))
        sf.setAttribute("f6", new SimpleDateFormat("yyyyMMdd").parse("20140102"))
        sf.setAttribute("f7", WKTUtils.read("POINT(45.0 49.0)"))
        sf.setAttribute("f8", WKTUtils.read("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))"))
        sf
      }

      val avroFile = writeAvroFile(features)
      val pipeFile = writePipeFile(features)

      val avroList = readAvroFile(sft, avroFile)
      val pipeList = readPipeFile(pipeFile, sft)

      foreach(Seq(avroList, pipeList))(_ must haveSize(features.length))

      foreach(features.zip(avroList).zip(pipeList)) { case ((feature, avro), pipe) =>
        avro mustEqual feature
        pipe mustEqual feature
      }
    }

    "read user data" in {
      val sf = createTypeWithGeo
      val sft = sf.getFeatureType

      val userData = sf.getUserData

      userData.put(SecurityUtils.FEATURE_VISIBILITY, "USER|ADMIN")
      userData.put(null, java.lang.Integer.valueOf(5))
      userData.put(java.lang.Long.valueOf(10), "10")
      userData.put("null", null)

      // serialize
      val baos = new ByteArrayOutputStream()
      val writer = new SimpleFeatureDatumWriter(sft, SerializationOptions.withUserData)
      val encoder = EncoderFactory.get().binaryEncoder(baos, null)
      writer.write(sf, encoder)
      encoder.flush()
      baos.close()

      val bytes = baos.toByteArray

      // deserialize
      val reader = SimpleFeatureDatumReader(writer.getSchema, sft)
      val decoder = DecoderFactory.get().binaryDecoder(bytes, 0, bytes.length, null)
      val result = reader.read(null, decoder)

      result must not(beNull)
      result.getFeatureType mustEqual sft
      result.getID mustEqual sf.getID
      result.getAttributes mustEqual sf.getAttributes
      result.getUserData mustEqual sf.getUserData
    }

    "read in a reasonable amount of time" in {
      logger.debug("Beginning Performance Testing against file...")
      val numFields = 60
      val numRecords = 1000
      logger.debug(f"Number of fields: $numFields%d")
      logger.debug(f"Number of records: $numRecords%d")

      val sft = SimpleFeatureTypes.createType("test", buildStringSchema(numFields))
      val sfList = Seq.tabulate(numRecords)(i => createStringFeatures(sft, numFields, i.toString))

      val avroFile = writeAvroFile(sfList)
      val pipeFile = writePipeFile(sfList)

      val pipeStart = System.currentTimeMillis()
      readPipeFile(pipeFile, sft)
      val pipeTime = System.currentTimeMillis() - pipeStart
      logger.debug(f"Text read time $pipeTime%dms")

      val avroStart = System.currentTimeMillis()
      readAvroFile(sft, avroFile)
      val avroTime = System.currentTimeMillis() - avroStart
      logger.debug(f"Avro read time $avroTime%dms")
      ok
    }
  }
}
