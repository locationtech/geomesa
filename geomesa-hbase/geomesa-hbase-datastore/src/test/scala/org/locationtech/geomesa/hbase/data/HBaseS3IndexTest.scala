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
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConnectionParam, HBaseCatalogParam}
import org.locationtech.geomesa.index.conf.QueryHints.{BIN_BATCH_SIZE, BIN_LABEL, BIN_SORT, BIN_TRACK}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseS3IndexTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  "S3Index" should {
    "work with HBase" in {
      val typeName = "testS3"

      val params = Map(
        ConnectionParam.getName -> MiniCluster.connection,
        HBaseCatalogParam.getName -> getClass.getSimpleName
      )
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(typeName) must beNull
        ds.createSchema(SimpleFeatureTypes.createType(typeName,
          "name:String,track:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled=s3:geom:dtg"))
        val sft = ds.getSchema(typeName)

        val features =
          (0 until 10).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track1", s"2010-05-07T0$i:00:00.000Z", s"POINT(4$i 60)")
          } ++ (10 until 20).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track2", s"2010-05-${i}T$i:00:00.000Z", s"POINT(4${i - 10} 60)")
          } ++ (20 until 30).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track3", s"2010-05-${i}T${i-10}:00:00.000Z", s"POINT(6${i - 20} 60)")
          }

        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(f => FeatureUtils.write(writer, f, useProvidedFid = true))
        }

        def runQuery(query: Query): Seq[SimpleFeature] =
          SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

        { // return all features for inclusive filter
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(10)
          features.map(_.getID.toInt) must containTheSameElementsAs(0 to 9)
        }

        { // return some features for exclusive geom filter
          val filter = "bbox(geom, 38, 59, 45, 61)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(6)
          features.map(_.getID.toInt) must containTheSameElementsAs(0 to 5)
        }

        { // return some features for exclusive date filter
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(4)
          features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
        }

        { // work with whole world filter
          val filter = "bbox(geom, -180, -90, 180, 90)" +
              " AND dtg between '2010-05-07T05:00:00.000Z' and '2010-05-07T08:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(4)
          features.map(_.getID.toInt) must containTheSameElementsAs(5 to 8)
        }

        { // work across week bounds
          val filter = "bbox(geom, 45, 59, 51, 61)" +
              " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-21T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(9)
          features.map(_.getID.toInt) must containTheSameElementsAs((6 to 9) ++ (15 to 19))
        }

        { // work across 2 weeks
          val filter = "bbox(geom, 44.5, 59, 50, 61)" +
              " AND dtg between '2010-05-10T00:00:00.000Z' and '2010-05-17T23:59:59.999Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(3)
          features.map(_.getID.toInt) must containTheSameElementsAs(15 to 17)
        }

        { // work with whole world filter across week bounds
          val filter = "bbox(geom, -180, -90, 180, 90)" +
              " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-21T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(15)
          features.map(_.getID.toInt) must containTheSameElementsAs(6 to 20)
        }

        { // work with whole world filter across 3 week periods
          val filter = "bbox(geom, -180, -90, 180, 90)" +
              " AND dtg between '2010-05-08T06:00:00.000Z' and '2010-05-30T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(20)
          features.map(_.getID.toInt) must containTheSameElementsAs(10 to 29)
        }

        { // work with small bboxes and date ranges
          val filter = "bbox(geom, 40.999, 59.999, 41.001, 60.001)" +
              " AND dtg between '2010-05-07T00:59:00.000Z' and '2010-05-07T01:01:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(1)
          features.head.getID.toInt mustEqual 1
        }

        { // support AND'ed GT/LT for dates
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg >= '2010-05-07T06:00:00.000Z' AND dtg <= '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(4)
          features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
        }

        { // apply secondary filters
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T05:00:00.000Z' and '2010-05-07T10:00:00.000Z'" +
              " AND name = 'name8'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(1)
          features.map(_.getID.toInt) must containTheSameElementsAs(Seq(8))
        }

        { // apply transforms
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter), Array("name")))
          features must haveSize(4)
          features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
          forall(features) { f =>
            f.getAttributeCount mustEqual 1
            f.getAttribute("name") must not(beNull)
          }
        }

        { // apply functional transforms
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter), Array("derived=strConcat('my', name)")))
          features must haveSize(4)
          features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
          forall(features) { f =>
            f.getAttributeCount mustEqual 1
            f.getAttribute("derived").asInstanceOf[String] must beMatching("myname\\d")
          }
        }

        { // optimize for bin format
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          query.getHints.put(BIN_TRACK, "name")
          query.getHints.put(BIN_BATCH_SIZE, 100)

          // reduce our scan ranges so that we get fewer iterator instances and some aggregation
          QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
          val aggregates = try {
            // the same simple feature gets reused - so make sure you access in serial order
            SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
                .map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
                .toList
          } finally {
            QueryProperties.ScanRangesTarget.threadLocalValue.remove()
          }
          aggregates.size must beLessThan(10) // ensure some aggregation was done
          val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
          bin must haveSize(10)
          bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
          bin.map(_.dtg) must
              containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
          forall(bin.map(_.lat))(_ mustEqual 60.0)
          bin.map(_.lon) must containAllOf((0 until 10).map(_ + 40.0f))
        }

        { // optimize for bin format with sorting
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          query.getHints.put(BIN_TRACK, "name")
          query.getHints.put(BIN_BATCH_SIZE, 100)
          query.getHints.put(BIN_SORT, true)

          // reduce our scan ranges so that we get fewer iterator instances and some aggregation
          QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
          val aggregates = try {
            // the same simple feature gets reused - so make sure you access in serial order
            SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
                .map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
                .toList
          } finally {
            QueryProperties.ScanRangesTarget.threadLocalValue.remove()
          }
          aggregates.size must beLessThan(10) // ensure some aggregation was done
          forall(aggregates) { a =>
            val window = a.grouped(16).map(BinaryOutputEncoder.decode(_).dtg).sliding(2).filter(_.length > 1)
            forall(window.toSeq)(w => w.head must beLessThanOrEqualTo(w(1)))
          }
          val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
          bin must haveSize(10)
          bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
          bin.map(_.dtg) must
              containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
          forall(bin.map(_.lat))(_ mustEqual 60.0)
          bin.map(_.lon) must containAllOf((0 until 10).map(_ + 40.0f))
        }

        { // optimize for bin format with label
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          query.getHints.put(BIN_TRACK, "name")
          query.getHints.put(BIN_LABEL, "name")
          query.getHints.put(BIN_BATCH_SIZE, 100)

          // reduce our scan ranges so that we get fewer iterator instances and some aggregation
          QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
          val aggregates = try {
            // the same simple feature gets reused - so make sure you access in serial order
            SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
                .map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
                .toList
          } finally {
            QueryProperties.ScanRangesTarget.threadLocalValue.remove()
          }
          aggregates.size must beLessThan(10) // ensure some aggregation was done
          val bin = aggregates.flatMap(a => a.grouped(24).map(BinaryOutputEncoder.decode))
          bin must haveSize(10)
          bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
          bin.map(_.dtg) must
              containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
          forall(bin.map(_.lat))(_ mustEqual 60.0)
          bin.map(_.lon) must containAllOf((0 until 10).map(_ + 40.0f))
          bin.map(_.label) must containAllOf((0 until 10).map(i => BinaryOutputEncoder.convertToLabel(s"name$i")))
        }

        { // optimize for bin format with transforms
          val filter = "bbox(geom, 38, 59, 51, 61)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter), Array("name", "geom"))
          query.getHints.put(BIN_TRACK, "name")
          query.getHints.put(BIN_BATCH_SIZE, 100)

          // reduce our scan ranges so that we get fewer iterator instances and some aggregation
          QueryProperties.ScanRangesTarget.threadLocalValue.set("1")
          val aggregates = try {
            // the same simple feature gets reused - so make sure you access in serial order
            SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
                .map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
                .toList
          } finally {
            QueryProperties.ScanRangesTarget.threadLocalValue.remove()
          }
          aggregates.size must beLessThan(10) // ensure some aggregation was done
          val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
          bin must haveSize(10)
          bin.map(_.trackId) must containAllOf((0 until 10).map(i => s"name$i".hashCode))
          bin.map(_.dtg) must
              containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
          forall(bin.map(_.lat))(_ mustEqual 60.0)
          bin.map(_.lon) must containAllOf((0 until 10).map(_ + 40.0f))
        }
      } finally {
        ds.dispose()
      }
    }
  }
}
