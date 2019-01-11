/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Point
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodedValues
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.process.transform.BinConversionProcess
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter

import scala.collection.JavaConversions._


class HBaseBinAggregatorTest extends HBaseTest with LazyLogging {
  sequential

  lazy val process = new BinConversionProcess
  lazy val TEST_HINT = new Hints()
  val sftName = "binTest"

  val spec = "name:String,track:String,dtg:Date,dtg2:Date,*geom:Point:srid=4326,geom2:Point:srid=4326"
  var sft = SimpleFeatureTypes.createType(sftName, spec)

  lazy val params = Map(
    ConnectionParam.getName -> connection,
    HBaseCatalogParam.getName -> catalogTableName)

  lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
  var fs: SimpleFeatureStore = _

  lazy val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, s"0$i")
    sf.setAttribute("name", s"name$i")
    sf.setAttribute("track", s"$i")
    sf.setAttribute("dtg", s"2017-02-20T00:00:0$i.000Z")
    sf.setAttribute("dtg2", s"2017-02-21T00:00:0$i.000Z")
    sf.setAttribute("geom", s"POINT(40 ${50 + i})")
    sf.setAttribute("geom2", s"POINT(20 ${30 + i})")
    sf
  }

  lazy val names   = features.map(_.getAttribute("name").hashCode)
  lazy val ids     = features.map(_.getID.hashCode)
  lazy val tracks  = features.map(_.getAttribute("track").hashCode)
  lazy val dates   = features.map(_.getAttribute("dtg").asInstanceOf[Date].getTime)
  lazy val dates2  = features.map(_.getAttribute("dtg2").asInstanceOf[Date].getTime)
  lazy val lonlat  = features.map(_.getAttribute("geom").asInstanceOf[Point]).map(p => (p.getY.toFloat, p.getX.toFloat))
  lazy val latlon  = lonlat.map(_.swap)
  lazy val lonlat2 = features.map(_.getAttribute("geom2").asInstanceOf[Point]).map(p => (p.getY.toFloat, p.getX.toFloat))
  lazy val latlon2 = lonlat2.map(_.swap)

  step {
    logger.info("Starting the Bin Aggregator Test")
    ds.getSchema(sftName) must beNull
    ds.createSchema(SimpleFeatureTypes.createType(sftName, spec))
    sft = ds.getSchema(sftName)
    fs = ds.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    features.foreach { f =>
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      featureCollection.add(f)
    }
    // write the feature to the store
    fs.addFeatures(featureCollection)
  }

  def toTuples(value: EncodedValues): Any = value match {
    case EncodedValues(trackId, lat, lon, dtg, label) if label == -1L => ((trackId, dtg), (lat, lon))
    case EncodedValues(trackId, lat, lon, dtg, label) => (((trackId, dtg), (lat, lon)), label)
  }

  "BinConversionProcess" should {
    "encode an accumulo feature collection in distributed fashion" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), "name", null, null, null, "lonlat").toList
      bytes.length must beLessThan(10)
      val decoded = bytes.reduceLeft(_ ++ _).grouped(16).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
      decoded must containTheSameElementsAs(names.zip(dates).zip(lonlat))
    }

    "encode an HBase feature collection in distributed fashion with alternate values" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), "name", "geom2", "dtg2", null, "lonlat").toList
      bytes.length must beLessThan(10)
      val decoded = bytes.reduceLeft(_ ++ _).grouped(16).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
      decoded must containTheSameElementsAs(names.zip(dates2).zip(lonlat2))
    }

    "encode an HBase feature collection in distributed fashion with labels" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), "name", null, null, "track", "lonlat").toList
      bytes.length must beLessThan(10)
      val decoded = bytes.reduceLeft(_ ++ _).grouped(24).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
      decoded must containTheSameElementsAs(names.zip(dates).zip(lonlat).zip(tracks))
    }
  }

  step {
    logger.info("Cleaning up HBase Bin Test")
    ds.dispose()
  }
}


