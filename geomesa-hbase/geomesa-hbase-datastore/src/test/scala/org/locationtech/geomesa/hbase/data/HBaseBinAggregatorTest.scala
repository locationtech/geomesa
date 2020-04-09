/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.process.transform.BinConversionProcess
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodedValues
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class HBaseBinAggregatorTest extends Specification with LazyLogging {
  sequential

  lazy val process = new BinConversionProcess
  lazy val TEST_HINT = new Hints()
  val sftName = "binTest"

  val spec = "name:String,track:String,dtg:Date,dtg2:Date,*geom:Point:srid=4326,geom2:Point:srid=4326"
  var sft = SimpleFeatureTypes.createType(sftName, spec)

  lazy val params = Map(
    HBaseDataStoreParams.ConnectionParam.getName   -> MiniCluster.connection,
    HBaseDataStoreParams.HBaseCatalogParam.getName -> getClass.getSimpleName,
    HBaseDataStoreParams.BinCoprocessorParam.key   -> true
  )

  lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
  lazy val dsSemiLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.BinCoprocessorParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsFullLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.RemoteFilteringParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsThreads1 = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.CoprocessorThreadsParam.key -> "1")).asInstanceOf[HBaseDataStore]
  lazy val dsYieldPartials = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.YieldPartialResultsParam.key -> true)).asInstanceOf[HBaseDataStore]
  lazy val dataStores = Seq(ds, dsSemiLocal, dsFullLocal, dsThreads1, dsYieldPartials)

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
    WithClose(ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    }
  }

  def toTuples(value: EncodedValues): Any = value match {
    case EncodedValues(trackId, lat, lon, dtg, label) if label == -1L => ((trackId, dtg), (lat, lon))
    case EncodedValues(trackId, lat, lon, dtg, label) => (((trackId, dtg), (lat, lon)), label)
  }

  "HBaseDataStoreFactory" should {
    "enable coprocessors" in {
      ds.config.remoteFilter must beTrue
      ds.config.coprocessors.enabled.bin must beTrue
      dsSemiLocal.config.remoteFilter must beTrue
      dsSemiLocal.config.coprocessors.enabled.bin must beFalse
      dsFullLocal.config.remoteFilter must beFalse
    }
  }

  "BinConversionProcess" should {
    "encode a feature collection in distributed fashion" in {
      foreach(dataStores) { ds =>
        val fc = ds.getFeatureSource(sftName).getFeatures(Filter.INCLUDE)
        val bytes = process.execute(fc, "name", null, null, null, "lonlat").toList
        bytes.length must beLessThan(10)
        val decoded = bytes.reduceLeft(_ ++ _).grouped(16).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
        decoded must containTheSameElementsAs(names.zip(dates).zip(lonlat))
      }
    }

    "encode a feature collection in distributed fashion with alternate values" in {
      foreach(dataStores) { ds =>
        val fc = ds.getFeatureSource(sftName).getFeatures(Filter.INCLUDE)
        val bytes = process.execute(fc, "name", "geom2", "dtg2", null, "lonlat").toList
        bytes.length must beLessThan(10)
        val decoded = bytes.reduceLeft(_ ++ _).grouped(16).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
        decoded must containTheSameElementsAs(names.zip(dates2).zip(lonlat2))
      }
    }

    "encode a feature collection in distributed fashion with labels" in {
      foreach(dataStores) { ds =>
        val fc = ds.getFeatureSource(sftName).getFeatures(Filter.INCLUDE)
        val bytes = process.execute(fc, "name", null, null, "track", "lonlat").toList
        bytes.length must beLessThan(10)
        val decoded = bytes.reduceLeft(_ ++ _).grouped(24).toSeq.map(BinaryOutputEncoder.decode).map(toTuples)
        decoded must containTheSameElementsAs(names.zip(dates).zip(lonlat).zip(tracks))
      }
    }
  }

  step {
    logger.info("Cleaning up HBase Bin Test")
    dataStores.foreach { _.dispose() }
  }
}


