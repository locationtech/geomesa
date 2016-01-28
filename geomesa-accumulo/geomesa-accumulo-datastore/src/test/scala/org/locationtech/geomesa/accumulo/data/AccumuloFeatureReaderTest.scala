/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.Connector
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.stats.{ParamsAuditProvider, QueryStat, Stat, StatWriter}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class AccumuloFeatureReaderTest extends Specification with TestWithDataStore {

  override def spec = s"name:String,dtg:Date,*geom:Point"

  val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(i.toString, sft)
    sf.setAttribute(0, s"name$i")
    sf.setAttribute(1, "2010-05-07T12:00:00.000Z")
    sf.setAttribute(2, "POINT(0 0)")
    sf
  }

  addFeatures(features)

  val filter = ECQL.toFilter("bbox(geom, -10, -10, 10, 10) and dtg during 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z")

  "AccumuloFeatureReader" should {
    "be able to collect stats" in {
      val stats = ArrayBuffer.empty[Stat]
      val query = new Query(sftName, filter)

      val qp = ds.getQueryPlanner(sftName)
      val sw = new StatWriter {
        override def getStatTable(stat: Stat): String = ???
        override def connector: Connector = ds.connector
        override def writeStat(stat: Stat): Unit = stats.append(stat)
      }

      val reader = AccumuloFeatureReader(query, qp, None, Some(sw), new ParamsAuditProvider)

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 10
      stats must haveLength(1)
      stats.head must beAnInstanceOf[QueryStat]
      stats.head.asInstanceOf[QueryStat].hits mustEqual 10
    }

    "be able to count bin results" in {
      val stats = ArrayBuffer.empty[Stat]
      val query = new Query(sftName, filter)
      query.getHints.put(QueryHints.BIN_TRACK_KEY, "name")
      query.getHints.put(QueryHints.BIN_BATCH_SIZE_KEY, 100)

      val qp = ds.getQueryPlanner(sftName)
      val sw = new StatWriter {
        override def getStatTable(stat: Stat): String = ???
        override def connector: Connector = ds.connector
        override def writeStat(stat: Stat): Unit = stats.append(stat)
      }

      val reader = AccumuloFeatureReader(query, qp, None, Some(sw), new ParamsAuditProvider)

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count must beLessThan(10)
      stats must haveLength(1)
      stats.head must beAnInstanceOf[QueryStat]
      stats.head.asInstanceOf[QueryStat].hits mustEqual 10
    }
  }
}
