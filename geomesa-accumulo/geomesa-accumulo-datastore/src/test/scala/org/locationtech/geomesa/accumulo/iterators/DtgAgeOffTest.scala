/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.{Collections, Date}

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.{DataStore, DataStoreFinder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DtgAgeOffTest extends Specification with TestWithDataStore {

  import scala.collection.JavaConverters._

  sequential

  override val spec = "dtg:Date,geom:Point:srid=4326"

  val today: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC)

  def add(ids: Range, ident: String, vis: String): Unit = {
    val features = ids.map { i =>
      ScalaSimpleFeature.create(sft, s"${ident}_$i", Date.from(today.minusDays(i).toInstant), s"POINT($i $i)")
    }
    features.foreach(SecurityUtils.setFeatureVisibility(_, vis))
    addFeatures(features)
  }

  def testDays(ds: DataStore, days: Int): Seq[SimpleFeature] = {
    ds.updateSchema(sft.getTypeName,
      SimpleFeatureTypes.immutable(sft, Collections.singletonMap(Configs.FeatureExpiration, s"dtg($days days)")))
    SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(Filter.INCLUDE).features).toList
  }

  "DTGAgeOff" should {

    "run at scan time" >> {
      add(1 to 10, "id", "A")
      testDays(ds, 11) must haveSize(10)
      testDays(ds, 10) must haveSize(9)
      testDays(ds, 5) must haveSize(4)
      testDays(ds, 1) must haveSize(0)
    }

    "respect vis with ageoff (vis trumps ageoff)" >> {
      // these exist but shouldn't be read!
      add(1 to 10, "anotherid", "D")
      testDays(ds, 11) must haveSize(10)
      testDays(ds, 10) must haveSize(9)
      testDays(ds, 5) must haveSize(4)
      testDays(ds, 1) must haveSize(0)

      val dsWithExtraAuth = {
        val connWithExtraAuth = {
          val mockInstance = new MockInstance("mycloud")
          val mockConnector = mockInstance.getConnector("user2", new PasswordToken("password2"))
          mockConnector.securityOperations().changeUserAuthorizations("user2", new Authorizations("A,B,C,D"))
          mockConnector
        }
        val params = dsParams ++ Map(AccumuloDataStoreParams.ConnectorParam.key -> connWithExtraAuth)
        DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
      }

      testDays(dsWithExtraAuth, 11) must haveSize(20)
      testDays(dsWithExtraAuth, 10) must haveSize(18)
      testDays(dsWithExtraAuth, 5) must haveSize(8)
      testDays(dsWithExtraAuth, 1) must haveSize(0)

      // these can be read
      add(1 to 10, "anotherid", "C")
      testDays(ds, 11) must haveSize(20)
      testDays(ds, 10) must haveSize(18)
      testDays(ds, 5) must haveSize(8)
      testDays(ds, 1) must haveSize(0)

      // these are 3x
      testDays(dsWithExtraAuth, 11) must haveSize(30)
      testDays(dsWithExtraAuth, 10) must haveSize(27)
      testDays(dsWithExtraAuth, 5) must haveSize(12)
      testDays(dsWithExtraAuth, 1) must haveSize(0)
    }
  }
}
