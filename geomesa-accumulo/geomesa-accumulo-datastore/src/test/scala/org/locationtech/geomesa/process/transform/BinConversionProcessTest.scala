/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import java.util.Date

import org.locationtech.jts.geom.Point
import org.geotools.data.collection.ListFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodedValues
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BinConversionProcessTest extends TestWithDataStore {

  import scala.collection.JavaConversions._

  sequential

  override val spec = "name:String,track:String,dtg:Date,dtg2:Date,*geom:Point:srid=4326,geom2:Point:srid=4326"

  val process = new BinConversionProcess

  val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, s"0$i")
    sf.setAttribute("name", s"name$i")
    sf.setAttribute("track", s"$i")
    sf.setAttribute("dtg", s"2017-02-20T00:00:0$i.000Z")
    sf.setAttribute("dtg2", s"2017-02-21T00:00:0$i.000Z")
    sf.setAttribute("geom", s"POINT(40 ${50 + i})")
    sf.setAttribute("geom2", s"POINT(20 ${30 + i})")
    sf
  }

  val ids     = features.map(_.getID.hashCode)
  val names   = features.map(_.getAttribute("name").hashCode)
  val tracks  = features.map(_.getAttribute("track").hashCode)
  val dates   = features.map(_.getAttribute("dtg").asInstanceOf[Date].getTime)
  val dates2  = features.map(_.getAttribute("dtg2").asInstanceOf[Date].getTime)
  val lonlat  = features.map(_.getAttribute("geom").asInstanceOf[Point]).map(p => (p.getY.toFloat, p.getX.toFloat))
  val latlon  = lonlat.map(_.swap)
  val lonlat2 = features.map(_.getAttribute("geom2").asInstanceOf[Point]).map(p => (p.getY.toFloat, p.getX.toFloat))
  val latlon2 = lonlat2.map(_.swap)

  addFeatures(features)

  // converts to tuples that we can compare to zipped values
  def toTuples(value: EncodedValues): Any = value match {
    case EncodedValues(trackId, lat, lon, dtg, label) if label == -1L => ((trackId, dtg), (lat, lon))
    case EncodedValues(trackId, lat, lon, dtg, label) => (((trackId, dtg), (lat, lon)), label)
  }

  "BinConversionProcess" should {
    "encode an empty feature collection" in {
      val bytes = process.execute(new ListFeatureCollection(sft), null, null, null, null, "lonlat")
      bytes must beEmpty
    }

    "encode an accumulo feature collection in distributed fashion" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), "name", null, null, null, "lonlat").toList
      bytes.length must beLessThan(10)
      val decoded = bytes.reduceLeft(_ ++ _).grouped(16).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
      decoded must containTheSameElementsAs(names.zip(dates).zip(lonlat))
    }

    "encode an accumulo feature collection in distributed fashion with alternate values" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), "name", "geom2", "dtg2", null, "lonlat").toList
      bytes.length must beLessThan(10)
      val decoded = bytes.reduceLeft(_ ++ _).grouped(16).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
      decoded must containTheSameElementsAs(names.zip(dates2).zip(lonlat2))
    }

    "encode an accumulo feature collection in distributed fashion with labels" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), "name", null, null, "track", "lonlat").toList
      bytes.length must beLessThan(10)
      val decoded = bytes.reduceLeft(_ ++ _).grouped(24).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
      decoded must containTheSameElementsAs(names.zip(dates).zip(lonlat).zip(tracks))
    }

    "encode an accumulo feature collection using feature id" in {
      failure("not implemented")
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), null, null, null, "track", "lonlat").toList
      bytes.length must beLessThan(10)
      val decoded = bytes.reduceLeft(_ ++ _).grouped(24).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
      decoded must containTheSameElementsAs(ids.zip(dates).zip(lonlat).zip(tracks))
    }.pendingUntilFixed
  }

}
