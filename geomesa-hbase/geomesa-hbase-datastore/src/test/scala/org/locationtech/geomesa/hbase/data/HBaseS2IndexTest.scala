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
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseS2IndexTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  "S2Index" should {
    "work with HBase" in {
      val typeName = "testS2"

      val params = Map(
        ConnectionParam.getName -> MiniCluster.connection,
        HBaseCatalogParam.getName -> getClass.getSimpleName)
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(typeName) must beNull
        ds.createSchema(SimpleFeatureTypes.createType(typeName,
          "name:String,track:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled=s2:geom"))
        val sft = ds.getSchema(typeName)

        val features =
          (0 until 10).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track1", s"2010-05-07T0$i:00:00.000Z", s"POINT(40 6$i)")
          } ++ (10 until 20).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track2", s"2010-05-${i}T$i:00:00.000Z", s"POINT(40 6${i - 10})")
          } ++ (20 until 30).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track3", s"2010-05-${i}T${i-10}:00:00.000Z", s"POINT(40 8${i - 20})")
          }

        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(f => FeatureUtils.write(writer, f, useProvidedFid = true))
        }

        def runQuery(query: Query): Seq[SimpleFeature] =
          SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

        { // return all features for inclusive filter
          val filter = "bbox(geom, 35, 55, 45, 75)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(10)
          features.map(_.getID.toInt) must containTheSameElementsAs(0 to 9)
        }

        { // return some features for exclusive geom filter
          val filter = "bbox(geom, 35, 55, 45, 65.001)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(6)
          features.map(_.getID.toInt) must containTheSameElementsAs(0 to 5)
        }

        { // return some features for exclusive date filter
          val filter = "bbox(geom, 35, 55, 45, 75)" +
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

        { // work with small bboxes
          val filter = "bbox(geom, 39.999, 60.999, 40.001, 61.001)"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(2)
          features.map(_.getID.toInt) must containTheSameElementsAs(Seq(1, 11))
        }

        { // apply secondary filters
          val filter = "bbox(geom, 35, 55, 45, 75)" +
              " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'" +
              " AND name = 'name8'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(1)
          features.map(_.getID.toInt) must containTheSameElementsAs(Seq(8))
        }

        { // apply transforms
          val filter = "bbox(geom, 35, 55, 45, 75)" +
              " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter), Array("name")))
          features must haveSize(4)
          features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
          forall(features)((f: SimpleFeature) => f.getAttributeCount mustEqual 1)
          forall(features)((f: SimpleFeature) => f.getAttribute("name") must not(beNull))
        }

        { // apply functional transforms
          val filter = "bbox(geom, 35, 55, 45, 75)" +
              " AND dtg between '2010-05-07T06:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter), Array("derived=strConcat('my', name)")))
          features must haveSize(4)
          features.map(_.getID.toInt) must containTheSameElementsAs(6 to 9)
          forall(features)((f: SimpleFeature) => f.getAttributeCount mustEqual 1)
          forall(features)((f: SimpleFeature) => f.getAttribute("derived").asInstanceOf[String] must beMatching("myname\\d"))
        }

        { // optimize for bin format
          val filter = "bbox(geom, -180, -90, 180, 90)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          query.getHints.put(BIN_TRACK, "name")
          query.getHints.put(BIN_BATCH_SIZE, 100)

          val returnedFeatures = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          // the same simple feature gets reused - so make sure you access in serial order
          val aggregates = returnedFeatures.map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toList
          aggregates.size must beLessThan(10) // ensure some aggregation was done
          val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
          bin must haveSize(10)
          (0 until 10).map(i => s"name$i".hashCode) must contain(atLeast(bin.map(_.trackId).tail: _*))
          bin.map(_.dtg) must
              containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
          bin.map(_.lat) must containAllOf((0 until 10).map(_ + 60.0f))
          forall(bin.map(_.lon))(_ mustEqual 40.0)
        }

        { // optimize for bin format with sorting
          val filter = "bbox(geom, -180, -90, 180, 90)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          query.getHints.put(BIN_TRACK, "name")
          query.getHints.put(BIN_BATCH_SIZE, 100)
          query.getHints.put(BIN_SORT, true)

          val returnedFeatures = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          // the same simple feature gets reused - so make sure you access in serial order
          val aggregates = returnedFeatures.map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toSeq
          aggregates.size must beLessThan(10) // ensure some aggregation was done
          forall(aggregates) { a =>
            val window = a.grouped(16).map(BinaryOutputEncoder.decode(_).dtg).sliding(2).filter(_.length > 1)
            forall(window.toSeq)(w => w.head must beLessThanOrEqualTo(w(1)))
          }
          val bin = aggregates.flatMap(a => a.grouped(16).map(BinaryOutputEncoder.decode))
          bin must haveSize(10)
          (0 until 10).map(i => s"name$i".hashCode) must contain(atLeast(bin.map(_.trackId).tail: _*))
          bin.map(_.dtg) must
              containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
          bin.map(_.lat) must containAllOf((0 until 10).map(_ + 60.0f))
          forall(bin.map(_.lon))(_ mustEqual 40.0)
        }

        { // optimize for bin format with label
          val filter = "bbox(geom, -180, -90, 180, 90)" +
              " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-07T12:00:00.000Z'"
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          query.getHints.put(BIN_TRACK, "name")
          query.getHints.put(BIN_LABEL, "name")
          query.getHints.put(BIN_BATCH_SIZE, 100)

          val returnedFeatures = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          // the same simple feature gets reused - so make sure you access in serial order
          val aggregates = returnedFeatures.map(f => f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toSeq
          aggregates.size must beLessThan(10) // ensure some aggregation was done
          val bin = aggregates.flatMap(a => a.grouped(24).map(BinaryOutputEncoder.decode))
          bin must haveSize(10)
          (0 until 10).map(i => s"name$i".hashCode) must contain(atLeast(bin.map(_.trackId).tail: _*))
          bin.map(_.dtg) must
              containAllOf((0 until 10).map(i => features(i).getAttribute("dtg").asInstanceOf[Date].getTime))
          bin.map(_.lat) must containAllOf((0 until 10).map(_ + 60.0f))
          forall(bin.map(_.lon))(_ mustEqual 40.0)
          bin.map(_.label) must containAllOf((0 until 10).map(i => BinaryOutputEncoder.convertToLabel(s"name$i")))
        }
      } finally {
        ds.dispose()
      }
    }
  }
}
