/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import java.io.{File, FileInputStream, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.UUID

import org.apache.avro.io.DecoderFactory
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.FeatureSpecificReader
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ListBuffer
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Version3CompatTest extends Specification {

  "Version3 ASF" should {

    val schema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,f8:Polygon:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", schema)

    def createV2Features(numFeatures : Int): Seq[Version2ASF] = {
      val r = new Random()
      r.setSeed(0)

      (0 until numFeatures).map { i =>
        val fid = new FeatureIdImpl(r.nextString(5))
        val sf = new Version2ASF(fid, sft)

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

    "read version 2 avro" >> {
      val v2Features = createV2Features(20)
      val f = File.createTempFile("avro", ".tmp")
      f.deleteOnExit()
      val fos = new FileOutputStream(f)
      v2Features.foreach { sf => sf.write(fos) }
      fos.close()

      val fis = new FileInputStream(f)
      val decoder = DecoderFactory.get().binaryDecoder(fis, null)
      val fsr = FeatureSpecificReader(sft)

      val sfList = new ListBuffer[SimpleFeature]()
      do {
        sfList += fsr.read(null, decoder)
      } while(!decoder.isEnd)

      fis.close()
      sfList.zip(v2Features).forall { case (v3, v2) =>
        v3.getAttributes mustEqual v2.getAttributes
      }

      f.delete
    }
  }
}
