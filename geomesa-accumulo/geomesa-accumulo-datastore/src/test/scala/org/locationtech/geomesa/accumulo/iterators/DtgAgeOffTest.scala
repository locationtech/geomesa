/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.DataStoreFinder
import org.geotools.factory.Hints
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class DtgAgeOffTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "some_id:String,dtg:Date,geom:Point:srid=4326"

  "DTGAgeOff" should {

    def configAgeOff(ads: AccumuloDataStore, days: Int): Unit = {

      val tOpt = ads.connector.tableOperations()
      val reloadedSft = ads.getSchema(sft.getTypeName)

      AccumuloFeatureIndex.indices(reloadedSft, IndexMode.Any).foreach { afi =>
        val tableName = afi.getTableName(reloadedSft.getTypeName, ds)
        tOpt.listIterators(tableName).filter(_._1 == "ageoff").foreach { case (i, e) =>
          tOpt.removeIterator(tableName, i, e)
        }

        val is = new IteratorSetting(5, "ageoff", classOf[DtgAgeOffIterator].getCanonicalName)
        is.addOption(AgeOffFilter.Options.SftOpt, SimpleFeatureTypes.encodeType(sft, true))
        is.addOption(DtgAgeOffIterator.Options.RetentionPeriodOpt, s"P${days}D")
        is.addOption(AgeOffFilter.Options.IndexOpt, afi.identifier)

        tOpt.attachIterator(tableName, is)
      }
    }

    val today: DateTime = DateTime.now(DateTimeZone.UTC)

    def createSF(i: Int, id: String, vis: Option[String]): SimpleFeature = {
      val geom = WKTUtils.read(s"POINT($i $i)")
      val arr = Array[AnyRef](
        id,
        today.minusDays(i).toDate,
        geom
      )
      val sf = ScalaSimpleFeature.create(sft, id, arr: _*)
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      vis.map(SecurityUtils.setFeatureVisibility(sf, _))
      sf
    }

    def testDays(d: Int): mutable.Buffer[SimpleFeature] = {
      configAgeOff(ds, d)
      import org.locationtech.geomesa.utils.geotools.Conversions._
      ds.getFeatureSource(sft.getTypeName).getFeatures(Filter.INCLUDE).features.toBuffer
    }

    "run at scan time" >> {
      addFeatures((1 to 10).map(i => createSF(i, s"id_$i", Some("A"))))
      testDays(11).size mustEqual 10
      testDays(10).size mustEqual 9
      testDays(5).size mustEqual 4
      testDays(1).size mustEqual 0

      success
    }

    "respect vis with ageoff (vis trumps ageoff)" >> {
      // these exist but shouldn't be read!
      addFeatures((1 to 10).map(i => createSF(i, s"anotherid_$i", Some("D"))))
      testDays(11).size mustEqual 10
      testDays(10).size mustEqual 9
      testDays(5).size mustEqual 4
      testDays(1).size mustEqual 0

      val dsWithExtraAuth = {
        val connWithExtraAuth = {
          val mockInstance = new MockInstance("mycloud")
          val mockConnector = mockInstance.getConnector("user2", new PasswordToken("password2"))
          mockConnector.securityOperations().changeUserAuthorizations("user2", new Authorizations("A,B,C,D"))
          mockConnector
        }
        import scala.collection.JavaConverters._
        DataStoreFinder.getDataStore(Map(
          "connector" -> connWithExtraAuth,
          "caching"   -> false,
          // note the table needs to be different to prevent testing errors
          "tableName" -> sftName).asJava).asInstanceOf[AccumuloDataStore]
      }

      def testWithExtraAuth(d: Int): mutable.Buffer[SimpleFeature] = {
        configAgeOff(dsWithExtraAuth, d)
        import org.locationtech.geomesa.utils.geotools.Conversions._
        dsWithExtraAuth.getFeatureSource(sft.getTypeName).getFeatures(Filter.INCLUDE).features.toBuffer
      }
      testWithExtraAuth(11).size mustEqual 20
      testWithExtraAuth(10).size mustEqual 18
      testWithExtraAuth(5).size mustEqual 8
      testWithExtraAuth(1).size mustEqual 0

      // these can be read
      addFeatures((1 to 10).map(i => createSF(i, s"anotherid_$i", Some("C"))))
      testDays(11).size mustEqual 20
      testDays(10).size mustEqual 18
      testDays(5).size mustEqual 8
      testDays(1).size mustEqual 0

      // these are 3x
      testWithExtraAuth(11).size mustEqual 30
      testWithExtraAuth(10).size mustEqual 27
      testWithExtraAuth(5).size mustEqual 12
      testWithExtraAuth(1).size mustEqual 0

      success
    }
  }
}