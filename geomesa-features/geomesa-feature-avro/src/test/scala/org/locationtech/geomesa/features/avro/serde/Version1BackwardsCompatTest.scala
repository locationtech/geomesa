/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.text.SimpleDateFormat
import java.util.UUID

import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.apache.avro.io.DecoderFactory
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.Assert
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.FeatureSpecificReader
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Codec.UTF8
import scala.io.Source
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Version1BackwardsCompatTest extends Specification {

  val geoFac = new GeometryFactory()
  def createTypeWithGeo: Version1ASF = {
    val sft = SimpleFeatureTypes.createType("test","f0:Point,f1:Polygon,f2:LineString")
    val sf = new Version1ASF(new FeatureIdImpl("fakeid"), sft)

    sf.setAttribute("f0", WKTUtils.read("POINT(45.0 49.0)"))
    sf.setAttribute("f1", WKTUtils.read("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))"))
    sf.setAttribute("f2", WKTUtils.read("LINESTRING(47.28515625 25.576171875, 48 26, 49 27)"))

    sf
  }

  def writeAvroFile(sfList: Seq[Version1ASF]): File = {
    val f = File.createTempFile("avro", ".tmp")
    f.deleteOnExit()
    val fos = new FileOutputStream(f)
    sfList.foreach { sf => sf.write(fos) }
    fos.close()
    f
  }

  def readAvroWithFsr(f: File, oldType: SimpleFeatureType): Seq[SimpleFeature] =
    readAvroWithFsr(f, oldType, oldType)

  def readAvroWithFsr(f: File, oldType: SimpleFeatureType, newType: SimpleFeatureType) = {
    val fis = new FileInputStream(f)
    val decoder = DecoderFactory.get().binaryDecoder(fis, null)
    val fsr = FeatureSpecificReader(oldType, newType)

    val sfList = new ListBuffer[SimpleFeature]()
    do {
      sfList += fsr.read(null, decoder)
    } while(!decoder.isEnd)

    fis.close()
    sfList.toList
  }

  def randomString(fieldId: Int, len: Int, r:Random) = {
    val sb = new mutable.StringBuilder()
    for (i <- 0 until len) {
      sb.append(fieldId)
    }
    sb.toString()
  }

  def createStringFeatures(schema: String, size: Int, id: String): Version1ASF = {
    val sft = SimpleFeatureTypes.createType("test", schema)
    val r = new Random()
    r.setSeed(0)

    var lst = new mutable.MutableList[String]
    for (i <- 0 until size) {
      lst += randomString(i, 8, r)
    }

    val sf = new Version1ASF(new FeatureIdImpl(id), sft)
    for (i <- 0 until lst.size) {
      sf.setAttribute(i, lst(i))
    }
    sf
  }

  def getSubsetData = {
    val numFields = 60
    val numRecords = 10
    val geoSchema = (0 until numFields).map { i => f"f$i%d:String" }.mkString(",")

    val sfSeq = for (i <- (0 until numRecords).toList) yield createStringFeatures(geoSchema, numFields,i.toString)

    sfSeq.foreach { sf => sf must beAnInstanceOf[Version1ASF] }
    val oldType = sfSeq(0).getType
    val f = writeAvroFile(sfSeq)
    val subsetType = SimpleFeatureTypes.createType("subsetType", "f0:String,f1:String,f3:String,f30:String,f59:String")
    val subsetList = readAvroWithFsr(f, oldType, subsetType)

    subsetList
  }

  def buildStringSchema(numFields: Int) = (0 until numFields).map { i => f"f$i%d:String" }.mkString(",")

  def writePipeFile(sfList: Seq[SimpleFeature]) = {
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

  def readPipeFile(f: File, sft: SimpleFeatureType) =
    Source.fromFile(f)(UTF8).getLines.map { line => DataUtilities.createFeature(sft, line) }.toList

  def createComplicatedFeatures(numFeatures : Int): Seq[Version1ASF] = {
    val geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,f8:Polygon:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", geoSchema)
    val r = new Random()
    r.setSeed(0)

    (0 until numFeatures).map { i =>
      val fid = new FeatureIdImpl(r.nextString(5))
      val sf = new Version1ASF(fid, sft)

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
  }

  "FeatureSpecificReader" should {
    "do subset data" in {
      val subset = getSubsetData
      subset.size mustEqual 10

      subset.foreach { sf =>
        // parsed as the new AvroSimpleFeature
        sf must beAnInstanceOf[ScalaSimpleFeature]

        sf.getAttributeCount mustEqual 5
        sf.getAttributes.size mustEqual 5

        import scala.collection.JavaConversions._
        sf.getAttributes.foreach { a =>
          a must not beNull
        }

        sf.getAttribute("f0") mustEqual "0"*8
        sf.getAttribute("f1") mustEqual "1"*8
        sf.getAttribute("f3") mustEqual "3"*8
        sf.getAttribute("f30") mustEqual "30"*8
        sf.getAttribute("f59") mustEqual "59"*8
      }
      success
    }

    "ensure a member in subset is null" in {
      getSubsetData(0).getAttribute("f20") must beNull
    }

    "handle geotypes" in {
      val orig = createTypeWithGeo
      val f = writeAvroFile(List(orig))
      val fsrList = readAvroWithFsr(f, orig.getType, orig.getType)

      fsrList.size mustEqual 1
      fsrList(0).getAttributeCount mustEqual 3
      fsrList(0).getAttributeCount mustEqual orig.getAttributeCount

      List("f0", "f1", "f2").foreach { f =>
        fsrList(0).getAttribute(f) mustEqual orig.getAttribute(f)
      }
      success
    }

    "deserialize properly compared to a pipe file" in {
      val numFields = 60
      val numRecords = 100
      val geoSchema = buildStringSchema(numFields)

      val sfList = for (i <- (0 until numRecords)) yield createStringFeatures(geoSchema, numFields, i.toString)

      val oldType = sfList(0).getType
      val avroFile = writeAvroFile(sfList)
      val pipeFile = writePipeFile(sfList)

      val subsetType = SimpleFeatureTypes.createType("subsetType", "f0:String,f1:String,f3:String,f30:String,f59:String")
      val fsrList = readAvroWithFsr(avroFile, oldType, subsetType)
      val pipeList = readPipeFile(pipeFile, oldType)

      sfList.size mustEqual pipeList.size
      fsrList.size mustEqual pipeList.size

      for(i <- 0 until sfList.size) {
        val f1 = sfList(i)
        val f2 = fsrList(i)
        val f3 = pipeList(i)

        f1.getID mustEqual f2.getID
        f1.getID mustEqual f3.getID

        f1.getAttributeCount mustEqual numFields
        f2.getAttributeCount mustEqual 5  //subset
        f3.getAttributeCount mustEqual numFields

        List("f0","f1", "f3", "f30", "f59").foreach { s =>
          f1.getAttribute(s) mustEqual f2.getAttribute(s)
          f2.getAttribute(s) mustEqual f3.getAttribute(s)
        }

        f1 mustNotEqual f2
      }
      success
    }

    "deserialize complex feature" in {
      val numRecords = 1
      val sfList = createComplicatedFeatures(numRecords)
      val oldType = sfList(0).getType

      val avroFile = writeAvroFile(sfList)
      val pipeFile = writePipeFile(sfList)

      val subsetType = SimpleFeatureTypes.createType("subsetType", "f0:String,f3:Float,f5:UUID,f6:Date")
      val pipeList = readPipeFile(pipeFile, oldType)
      val avroList = readAvroWithFsr(avroFile, oldType, subsetType)

      avroList.size mustEqual pipeList.size
      avroList.size mustEqual numRecords

      for(i <- 0 until numRecords){
        val a = pipeList(i)
        val b = avroList(i)
        List("f0","f3", "f5", "f6").foreach { s =>
          Assert.assertEquals(a.getAttribute(s), b.getAttribute(s))
          Assert.assertEquals(a.getAttribute(s), sfList(i).getAttribute(s))
        }
      }
      success
    }

    "properly skip geoms from version 1" in {
      val sft = SimpleFeatureTypes.createType("test", "a:Point,b:Point")
      val bOnly = SimpleFeatureTypes.createType("bonly", "b:Point")

      val v1 = new Version1ASF(new FeatureIdImpl("fakeid"), sft)
      v1.setAttribute("a", WKTUtils.read("POINT(2 2)"))
      v1.setAttribute("b", WKTUtils.read("POINT(45 56)"))

      val baos = new ByteArrayOutputStream()
      v1.write(baos)

      val bais = new ByteArrayInputStream(baos.toByteArray)

      val fsr = FeatureSpecificReader(sft, bOnly)
      val asf = fsr.read(null, DecoderFactory.get.directBinaryDecoder(bais, null))

      asf.getAttributeCount mustEqual 1
      asf.getAttribute(0).asInstanceOf[Geometry] mustEqual WKTUtils.read("POINT(45 56)")
    }

    "properly handle null geoms from version 1" in {
      val sft = SimpleFeatureTypes.createType("test", "a:Point,b:Point")
      val v2 = new Version1ASF(new FeatureIdImpl("fake2"), sft)
      v2.setAttribute("b", WKTUtils.read("POINT(45 56)"))

      val baos2 = new ByteArrayOutputStream()
      v2.write(baos2)

      val bais2 = new ByteArrayInputStream(baos2.toByteArray)
      val fsr2 = FeatureSpecificReader(sft)
      val asf2 = fsr2.read(null, DecoderFactory.get.directBinaryDecoder(bais2, null))

      asf2.getAttributeCount mustEqual 2
      asf2.getAttribute(0) must beNull
      asf2.getAttribute(1).asInstanceOf[Geometry] mustEqual WKTUtils.read("POINT(45 56)")
    }
  }

}
