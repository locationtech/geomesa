/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{Query, _}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class HBaseDensityFilterTest extends Specification with LazyLogging {

  sequential

  val TEST_FAMILY = "an_id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Point:srid=4326"
  val TEST_HINT = new Hints()
  val typeName = "HBaseDensityFilterTest"

  lazy val params = Map(
    HBaseDataStoreParams.ConnectionParam.getName     -> MiniCluster.connection,
    HBaseDataStoreParams.HBaseCatalogParam.getName   -> getClass.getSimpleName,
    HBaseDataStoreParams.DensityCoprocessorParam.key -> true
  )

  lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
  lazy val dsSemiLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.DensityCoprocessorParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsFullLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.RemoteFilteringParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsThreads1 = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.CoprocessorThreadsParam.key -> "1")).asInstanceOf[HBaseDataStore]
  lazy val dsYieldPartials = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.YieldPartialResultsParam.key -> true)).asInstanceOf[HBaseDataStore]
  lazy val dataStores = Seq(ds, dsSemiLocal, dsFullLocal, dsThreads1, dsYieldPartials)

  var sft: SimpleFeatureType = _

  step {
    logger.info("Starting the Density Filter Test")
    ds.getSchema(typeName) must beNull
    ds.createSchema(SimpleFeatureTypes.createType(typeName, TEST_FAMILY))
    sft = ds.getSchema(typeName)
  }

  "HBaseDataStoreFactory" should {
    "enable coprocessors" in {
      ds.config.remoteFilter must beTrue
      ds.config.coprocessors.enabled.density must beTrue
      dsSemiLocal.config.remoteFilter must beTrue
      dsSemiLocal.config.coprocessors.enabled.density must beFalse
      dsFullLocal.config.remoteFilter must beFalse
    }
  }

  "HBaseDensityCoprocessor" should {
    "work with filters" in {
      clearFeatures()

      val toAdd = (0 until 150).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, "2012-01-01T19:00:00Z")
        sf.setAttribute(3, "POINT(-77 38)")
        sf
      }  :+ {
        val sf2 = new ScalaSimpleFeature(sft, "200")
        sf2.setAttribute(0, "200")
        sf2.setAttribute(1, "1.0")
        sf2.setAttribute(2, "2010-01-01T19:00:00Z")
        sf2.setAttribute(3, "POINT(1 1)")
        sf2
      }

      addFeatures(toAdd)

      val q = "BBOX(geom, 0, 0, 10, 10)"
      foreach(dataStores) { ds =>
        val density = getDensity(typeName, q, ds)
        density.length must equalTo(1)
      }
    }

    "reduce total features returned" in {
      clearFeatures()

      val toAdd = (0 until 150).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, "2012-01-01T19:00:00Z")
        sf.setAttribute(3, "POINT(-77 38)")
        sf
      }

      addFeatures(toAdd)

      val q = "(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)"
      foreach(dataStores) { ds =>
        val density = getDensity(typeName, q, ds)
        density.length must beLessThan(150)
        density.map(_._3).sum must beEqualTo(150)
      }
    }

    "maintain total weight of points" in {
      clearFeatures()

      val toAdd = (0 until 150).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, "2012-01-01T19:00:00Z")
        sf.setAttribute(3, "POINT(-77 38)")
        sf
      }

      addFeatures(toAdd)

      val q = "(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)"
      foreach(dataStores) { ds =>
        val density = getDensity(typeName, q, ds)
        density.length must beLessThan(150)
        density.map(_._3).sum must beEqualTo(150)
      }
    }

    "maintain weights irrespective of dates" in {
      clearFeatures()

      val toAdd = (0 until 150).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, Date.from(ZonedDateTime.of(2012, 1, 1, 19, 0, 0, 0, ZoneOffset.UTC).plusSeconds(i).toInstant))
        sf.setAttribute(3, "POINT(-77 38)")
        sf
      }

      addFeatures(toAdd)

      val q = "(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)"
      foreach(dataStores) { ds =>
        val density = getDensity(typeName, q, ds)
        density.length must beLessThan(150)
        density.map(_._3).sum must beEqualTo(150)
      }
    }

    "correctly bin points" in {
      clearFeatures()

      val toAdd = (0 until 150).map { i =>
        // space out the points very slightly around 5 primary latitudes 1 degree apart
        val lat = (i / 30) + 1 + (Random.nextDouble() - 0.5) / 1000.0
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, Date.from(ZonedDateTime.of(2012, 1, 1, 19, 0, 0, 0, ZoneOffset.UTC).plusSeconds(i).toInstant))
        sf.setAttribute(3, s"POINT($lat 37)")
        sf
      }

      addFeatures(toAdd)

      val q = "(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -1, 33, 6, 40)"
      foreach(dataStores) { ds =>
        val density = getDensity(typeName, q, ds)
        density.map(_._3).sum mustEqual 150

        val compiled = density.groupBy(d => (d._1, d._2)).map { case (_, group) => group.map(_._3).sum }

        // should be 5 bins of 30
        compiled must haveLength(5)
        forall(compiled)(_ mustEqual 30)
      }
    }
  }

  step {
    logger.info("Cleaning up HBase Density Test")
    dataStores.foreach { _.dispose() }
  }

  def addFeatures(features: Seq[SimpleFeature]): Unit = {
    WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    }
  }

  def clearFeatures(): Unit = {
    val writer = ds.getFeatureWriter(typeName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
    while (writer.hasNext) {
      writer.next()
      writer.remove()
    }
    writer.close()
  }

  def getDensity(typeName: String, query: String, ds: DataStore): List[(Double, Double, Double)] = {
    val filter = ECQL.toFilter(query)
    val envelope = FilterHelper.extractGeometries(filter, "geom").values.headOption match {
      case None    => ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)
      case Some(g) => ReferencedEnvelope.create(g.getEnvelopeInternal,  DefaultGeographicCRS.WGS84)
    }
    val q = new Query(typeName, filter)
    q.getHints.put(QueryHints.DENSITY_BBOX, envelope)
    q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
    q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
    val decode = DensityScan.decodeResult(envelope, 500, 500)
    SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).flatMap(decode).toList
  }
}