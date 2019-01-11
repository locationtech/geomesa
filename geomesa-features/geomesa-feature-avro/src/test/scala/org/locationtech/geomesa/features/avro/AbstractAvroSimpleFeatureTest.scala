/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.UUID

import org.locationtech.jts.geom.{Point, Polygon}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.avro.serde.Version2ASF
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

trait AbstractAvroSimpleFeatureTest {

  val complexSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,"+
    "f8:Polygon:srid=4326,f9:Long,f10:String,f11:Integer,f12:Date,f13:Geometry,f14:UUID,f15:Bytes"
  val complexSft = SimpleFeatureTypes.createType("test", complexSchema)

  val filesCreated = ArrayBuffer.empty[File]

  def createComplicatedFeatures(numFeatures : Int) : List[Version2ASF] = {
    val r = new Random(0)

    val list = new ListBuffer[Version2ASF]
    for(i <- 0 until numFeatures){
      val fid = new FeatureIdImpl(r.nextString(5))
      val sf = new Version2ASF(fid, complexSft)

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

      val bytes = "FOOBARBAZ+12354+\u0000\u0001\u0002\u3434".getBytes(StandardCharsets.UTF_8)
      sf.setAttribute("f15", bytes.asInstanceOf[Object])

      list += sf
    }
    list.toList
  }

  val simpleSft = SimpleFeatureTypes.createType("AvroSimpleFeatureWriterTest", "name:String,*geom:Point,dtg:Date")

  def createSimpleFeature: SimpleFeature = {
    val builder = AvroSimpleFeatureFactory.featureBuilder(simpleSft)
    builder.reset()
    builder.set("name", "test_feature")
    builder.set("geom", WKTUtils.read("POINT(-110 30)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")

    val sf = builder.buildFeature("fid")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  def getFeatures(f: File) = {
    val dfr = new AvroDataFileReader(new FileInputStream(f))
    try {
      dfr.toList
    } finally {
      dfr.close()
    }
  }

  /**
    * Client should delete file
    */
  def getTmpFile = {
    val tmpFile = File.createTempFile("geomesa", "avro")
    tmpFile.deleteOnExit()
    tmpFile.createNewFile()
    filesCreated.append(tmpFile)
    tmpFile
  }

}
