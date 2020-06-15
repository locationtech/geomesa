/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.ipc.message.IpcOption
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.iterators.ArrowScan.MultiFileAggregate
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowScanTest extends Specification with LazyLogging {
  val typeName = "attr-idx-test"
  val spec = "name:String:index=true,age:Int:index=true,height:Float:index=true,dtg:Date,*geom:Point:srid=4326"

  val sft = SimpleFeatureTypes.createType(typeName, spec)

  val aliceGeom   = WKTUtils.read("POINT(45.0 49.0)")
  val billGeom    = WKTUtils.read("POINT(46.0 49.0)")
  val bobGeom     = WKTUtils.read("POINT(47.0 49.0)")
  val charlesGeom = WKTUtils.read("POINT(48.0 49.0)")

  val aliceDate   = Converters.convert("2012-01-01T12:00:00.000Z", classOf[java.util.Date])
  val billDate    = Converters.convert("2013-01-01T12:00:00.000Z", classOf[java.util.Date])
  val bobDate     = Converters.convert("2014-01-01T12:00:00.000Z", classOf[java.util.Date])
  val charlesDate = Converters.convert("2014-01-01T12:30:00.000Z", classOf[java.util.Date])

  val features = Seq(
    Array("alice",   20,   10f, aliceDate,   aliceGeom),
    Array("bill",    21,   11f, billDate,    billGeom),
    Array("bob",     30,   12f, bobDate,     bobGeom),
    Array("charles", null, 12f, charlesDate, charlesGeom)
  ).map { entry =>
    ScalaSimpleFeature.create(sft, entry.head.toString, entry: _*)
  }

  "ArrowScan's MultiFileAggregate" should {
    "be able to be reused" in {

      val mfa = new MultiFileAggregate(sft, Nil, SimpleFeatureEncoding.min(includeFids = true), new IpcOption())

      mfa.init()
      mfa.aggregate(features(0))
      mfa.aggregate(features(1))
      val bytes1 = mfa.encode()
      mfa.aggregate(features(2))
      mfa.aggregate(features(3))
      val bytes2 = mfa.encode()

      readFeatures(bytes1) mustEqual 2
      readFeatures(bytes2) mustEqual 2

      def readFeatures(bytes: Array[Byte]) = {
        var count = 0
        val innerOut = new ByteArrayOutputStream
        innerOut.write(bytes)
        def innnerIn() = new ByteArrayInputStream(innerOut.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(innnerIn)) { reader =>
          SelfClosingIterator(reader.features()).foreach {
            f =>
              count += 1
              logger.debug(s"\tFeature: ${f.getID}: ${f.getAttributes}")
          }
        }
        count
      }

      mfa.cleanup()
      ok
    }
  }
}
