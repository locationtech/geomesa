/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.iterators.AttrKeyPlusValueIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AttrKeyPlusValueIteratorNoSharingTest extends Specification with TestWithDataStore {

  override val spec =
    "name:String:index=true:cardinality=high," +
    "age:Integer:index=false," +
    "count:Long:index=false," +
    "dtg:Date:default=true," +
    "*geom:Point:srid=4326"

  override val tableSharing = false

  val dtf = ISODateTimeFormat.dateTime().withZoneUTC()

  val features = Seq(
    Array("alice",   20,   1, dtf.parseDateTime("2014-01-01T12:00:00.000Z").toDate, WKTUtils.read("POINT(45.0 49.0)")),
    Array("bill",    21,   2, dtf.parseDateTime("2014-01-02T12:00:00.000Z").toDate, WKTUtils.read("POINT(46.0 49.0)")),
    Array("bob",     30,   3, dtf.parseDateTime("2014-01-03T12:00:00.000Z").toDate, WKTUtils.read("POINT(47.0 49.0)")),
    Array("charles", null, 4, dtf.parseDateTime("2014-01-04T12:00:00.000Z").toDate, WKTUtils.read("POINT(48.0 49.0)"))
  ).map { entry =>
    val feature = new ScalaSimpleFeature(entry.head.toString, sft)
    feature.setAttributes(entry.asInstanceOf[Array[AnyRef]])
    feature
  }

  addFeatures(features)

  val queryPlanner = new QueryPlanner(sft, ds.getFeatureEncoding(sft), ds.getIndexSchemaFmt(sftName), ds,
    ds.strategyHints(sft))

  "Query planning" should {

    "be using un-shared tables" >> {
      // TODO GEOMESA-1146 refactor to allow running of tests with table sharing on and off...
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      sft.isTableSharing must beFalse
    }

    "do a single scan for attribute idx queries" >> {
      val filterName = "(((name = 'alice') or name = 'bill') or name = 'bob')"
      val filter = ECQL.toFilter(s"$filterName AND BBOX(geom, 40, 40, 60, 60) and dtg during 2014-01-01T00:00:00.000Z/2014-01-05T00:00:00.000Z ")
      val query = new Query(sftName, filter, Array[String]("dtg", "geom", "name"))
      val plans = ds.getQueryPlan(query)
      plans.size mustEqual 1
      plans.head must beAnInstanceOf[BatchScanPlan]
      val bsp = plans.head.asInstanceOf[BatchScanPlan]
      bsp.iterators.size mustEqual 1
      bsp.iterators.head.getIteratorClass mustEqual classOf[AttrKeyPlusValueIterator].getName

      import org.locationtech.geomesa.utils.geotools.Conversions._
      val rws = fs.getFeatures(query).features().toList
      rws.size mustEqual 3
      val alice = rws.filter(_.get[String]("name") == "alice").head
      alice.getID mustEqual "alice"
      alice.getAttributeCount mustEqual 3
    }

    "work with 150 attrs" >> {
      val filterName = "(" + (0 to 150).map(i => i.toString).map(i => s"name = '$i'").mkString(" or ") + " )"
      val filter = ECQL.toFilter(s"$filterName AND BBOX(geom, 40, 40, 60, 60) and dtg during 2014-01-01T00:00:00.000Z/2014-01-05T00:00:00.000Z ")

      val query = new Query(sftName, filter, Array[String]("dtg", "geom", "name"))
      val plans = ds.getQueryPlan(query)
      plans.size mustEqual 1
      plans.head must beAnInstanceOf[BatchScanPlan]
      val bsp = plans.head.asInstanceOf[BatchScanPlan]
      bsp.iterators.size mustEqual 1
      bsp.iterators.head.getIteratorClass mustEqual classOf[AttrKeyPlusValueIterator].getName
    }
  }
}
