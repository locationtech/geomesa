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
import org.locationtech.geomesa.accumulo.index.{QueryHints, QueryPlanner}
import org.locationtech.geomesa.accumulo.stats.{ParamsAuditProvider, QueryStat, Stat, StatWriter}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class AccumuloFeatureReaderTest extends Specification with TestWithDataStore {

  override def spec = s"name:String,dtg:Date,*geom:Point"

  val features = (0 until 100).map { i =>
    val sf = new ScalaSimpleFeature(i.toString, sft)
    sf.setAttribute(0, s"name$i")
    sf.setAttribute(1, f"2010-05-07T${i % 24}%02d:01:00.000Z")
    sf.setAttribute(2, s"POINT(${i % 10} ${i % 5})")
    sf
  }

  addFeatures(features)

  val filter = ECQL.toFilter("bbox(geom, -10, -10, 10, 10) and dtg during 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z")

  def dataStoreWithStats(statArray: ArrayBuffer[Stat]) =
    new AccumuloDataStore(ds.connector, ds.catalogTable, ds.authProvider,
      ds.auditProvider, ds.defaultVisibilities, ds.config) with StatWriter {
    override def writeStat(stat: Stat): Unit = statArray.append(stat)
  }

  "AccumuloFeatureReader" should {
    "be able to run without stats" in {
      val query = new Query(sftName, filter)

      val qp = ds.getQueryPlanner(sftName)
      val reader = AccumuloFeatureReader(query, qp, None, None)

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 100
    }

    "be able to collect stats" in {
      val stats = ArrayBuffer.empty[Stat]
      val query = new Query(sftName, filter)

      val qp = ds.getQueryPlanner(sftName)
      val sw = new StatWriter {
        override def getStatTable(stat: Stat): String = ???
        override def connector: Connector = ds.connector
        override def writeStat(stat: Stat): Unit = stats.append(stat)
      }

      val reader = AccumuloFeatureReader(query, qp, None, Some(sw, new ParamsAuditProvider))

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 100
      stats must haveLength(1)
      stats.head must beAnInstanceOf[QueryStat]
      stats.head.asInstanceOf[QueryStat].hits mustEqual 100
    }

    "be able to count bin results in stats" in {
      val stats = ArrayBuffer.empty[Stat]
      val query = new Query(sftName, filter)
      query.getHints.put(QueryHints.BIN_TRACK_KEY, "name")
      query.getHints.put(QueryHints.BIN_BATCH_SIZE_KEY, 10)

      val qp = ds.getQueryPlanner(sftName)
      val sw = new StatWriter {
        override def getStatTable(stat: Stat): String = ???
        override def connector: Connector = ds.connector
        override def writeStat(stat: Stat): Unit = stats.append(stat)
      }

      val reader = AccumuloFeatureReader(query, qp, None, Some(sw, new ParamsAuditProvider))

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count must beLessThan(100)
      stats must haveLength(1)
      stats.head must beAnInstanceOf[QueryStat]
      stats.head.asInstanceOf[QueryStat].hits mustEqual 100
    }

    "be able to count bin results in stats through geoserver" in {
      val stats = ArrayBuffer.empty[Stat]
      val query = new Query(sftName, filter)

      val collection = dataStoreWithStats(stats).getFeatureSource(sftName).getFeatures(query)

      // put the hints in the request after getting the feature collection
      // to mimic the bin output format workflow
      val hints = Map(QueryHints.BIN_TRACK_KEY -> "name", QueryHints.BIN_BATCH_SIZE_KEY -> 10)
      QueryPlanner.setPerThreadQueryHints(hints.asInstanceOf[Map[AnyRef, AnyRef]])

      val reader = collection.features()

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count must beLessThan(100)
      stats must haveLength(1)
      stats.head must beAnInstanceOf[QueryStat]
      stats.head.asInstanceOf[QueryStat].hits mustEqual 100
    }

    "be able to limit features" in {
      val query = new Query(sftName, filter)
      query.setMaxFeatures(10)

      val qp = ds.getQueryPlanner(sftName)
      val reader = AccumuloFeatureReader(query, qp, None, None)

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 10
    }

    "be able to limit features in bin results" in {
      val query = new Query(sftName, filter)
      query.getHints.put(QueryHints.BIN_TRACK_KEY, "name")
      query.getHints.put(QueryHints.BIN_BATCH_SIZE_KEY, 10)
      query.setMaxFeatures(10)

      val qp = ds.getQueryPlanner(sftName)
      val reader = AccumuloFeatureReader(query, qp, None, None)

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count must beLessThan(20) // at batch size of 10 we should have less than 2 full batches
    }

    "be able to limit  features in bin results through geoserver" in {
      val query = new Query(sftName, filter)
      query.setMaxFeatures(10)

      val collection = ds.getFeatureSource(sftName).getFeatures(query)

      val hints = Map(QueryHints.BIN_TRACK_KEY -> "name", QueryHints.BIN_BATCH_SIZE_KEY -> 10)
      QueryPlanner.setPerThreadQueryHints(hints.asInstanceOf[Map[AnyRef, AnyRef]])

      val reader = collection.features()

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count must beLessThan(20) // at batch size of 10 we should have less than 2 full batches
    }

    "be able to limit features and collect stats" in {
      val stats = ArrayBuffer.empty[Stat]
      val query = new Query(sftName, filter)
      query.setMaxFeatures(10)

      val qp = ds.getQueryPlanner(sftName)
      val sw = new StatWriter {
        override def getStatTable(stat: Stat): String = ???
        override def connector: Connector = ds.connector
        override def writeStat(stat: Stat): Unit = stats.append(stat)
      }

      val reader = AccumuloFeatureReader(query, qp, None, Some(sw, new ParamsAuditProvider))

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count mustEqual 10
      stats must haveLength(1)
      stats.head must beAnInstanceOf[QueryStat]
      stats.head.asInstanceOf[QueryStat].hits mustEqual 10
    }

    "be able to limit features in bin results and collect stats" in {
      val stats = ArrayBuffer.empty[Stat]
      val query = new Query(sftName, filter)
      query.getHints.put(QueryHints.BIN_TRACK_KEY, "name")
      query.getHints.put(QueryHints.BIN_BATCH_SIZE_KEY, 10)
      query.setMaxFeatures(10)

      val qp = ds.getQueryPlanner(sftName)
      val sw = new StatWriter {
        override def getStatTable(stat: Stat): String = ???
        override def connector: Connector = ds.connector
        override def writeStat(stat: Stat): Unit = stats.append(stat)
      }

      val reader = AccumuloFeatureReader(query, qp, None, Some(sw, new ParamsAuditProvider))

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      count must beLessThan(10)
      stats must haveLength(1)
      stats.head must beAnInstanceOf[QueryStat]
      stats.head.asInstanceOf[QueryStat].hits must beLessThan(20L)
    }

    "be able to limit  features in bin results and collect stats through geoserver" in {
      val stats = ArrayBuffer.empty[Stat]
      val query = new Query(sftName, filter)
      query.setMaxFeatures(10)

      val collection = dataStoreWithStats(stats).getFeatureSource(sftName).getFeatures(query)

      // put the hints in the request after getting the feature collection
      // to mimic the bin output format workflow
      val hints = Map(QueryHints.BIN_TRACK_KEY -> "name", QueryHints.BIN_BATCH_SIZE_KEY -> 10)
      QueryPlanner.setPerThreadQueryHints(hints.asInstanceOf[Map[AnyRef, AnyRef]])

      val reader = collection.features()

      var count = 0
      while (reader.hasNext) { reader.next(); count += 1 }
      reader.close()

      // the features that get returned depend on the ranges that get set up in accumulo,
      // as each range will create a new bin iterator and return aggregated features for that range.
      // Since each feature can have 0-10 bin records (based on the bin batch size hint), we
      // don't know exactly how many features will get returned before the bin limit of 10
      // (based on maxFeatures) is hit. But conservatively, if there is at least one feature
      // that contains 2+ bin records, there will be less than 10 features.

      count must beLessThan(10)

      // in the stat tracking, the max bin records that can be returned is 19. This would be if
      // the first feature had 9 records and then the second feature had 10 records, triggering the limit.

      stats must haveLength(1)
      stats.head must beAnInstanceOf[QueryStat]
      stats.head.asInstanceOf[QueryStat].hits must beLessThan(20L)
    }
  }
}
