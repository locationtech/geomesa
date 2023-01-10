/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import org.geotools.data.{DataStoreFinder, DataUtilities, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.{Collections, Date}

@RunWith(classOf[JUnitRunner])
class RedisDataStoreIntegrationTest extends Specification {

  import scala.collection.JavaConverters._

  sequential

  val url = "redis://localhost:6379"

  val sft = SimpleFeatureTypes.createImmutableType("test", "name:String:index=true,dtg:Date,*geom:Point:srid=4326")

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, i.toString, s"name$i", s"2019-01-03T0$i:00:00.000Z", s"POINT (-4$i 55)")
  }

  val filters = Seq(
    "bbox(geom, -39, 54, -51, 56)",
    "bbox(geom, -45, 54, -49, 56)",
    "bbox(geom, -39, 54, -51, 56) AND dtg >= '2019-01-03T00:00:00.000Z' AND dtg < '2019-01-03T12:00:00.000Z'",
    "bbox(geom, -45, 54, -49, 56) AND dtg >= '2019-01-03T00:00:00.000Z' AND dtg < '2019-01-03T12:00:00.000Z'",
    "bbox(geom, -39, 54, -51, 56) AND dtg during 2019-01-03T04:30:00.000Z/2019-01-03T08:30:00.000Z",
    s"name IN('${features.map(_.getAttribute("name")).mkString("', '")}')",
    "name IN('name0', 'name2') AND dtg >= '2019-01-03T00:00:00.000Z' AND dtg < '2019-01-03T01:00:00.000Z'",
    features.map(_.getID).mkString("IN('", "', '", "')")
  ).map(ECQL.toFilter)

  val transforms = Seq(null, Array("dtg", "geom"), Array("name", "geom"))

  val params = Map(
    RedisDataStoreParams.RedisUrlParam.key -> url,
    RedisDataStoreParams.RedisCatalogParam.key -> "gm-test",
    RedisDataStoreParams.PipelineParam.key -> "false" // "true"
  )

  "RedisDataStore" should {
    "read and write features" in {

      skipped("Integration tests")

      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(sft.getTypeName) must beNull
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        foreach(filters) { filter =>
          val filtered = features.filter(filter.evaluate)
          foreach(transforms) { transform =>
            val query = new Query(sft.getTypeName, filter, transform: _*)
            val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
            val expected = if (transform == null) { filtered } else {
              val tsft = DataUtilities.createSubType(sft, transform: _*)
              filtered.map(DataUtilities.reType(tsft, _)).map(ScalaSimpleFeature.copy)
            }
            result must containTheSameElementsAs(expected)
          }
        }

        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-49, -40, 55, 55, CRS_EPSG_4326)
      } finally {
        ds.removeSchema(sft.getTypeName)
        ds.dispose()
      }
    }

    "expire features based on ingest time" in {

      skipped("Integration tests")

      RedisSystemProperties.AgeOffInterval.set("5 seconds")

      val sft = SimpleFeatureTypes.immutable(this.sft, Collections.singletonMap(Configs.FeatureExpiration, "10 seconds"))
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(sft.getTypeName) must beNull
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        foreach(filters) { filter =>
          val expected = features.filter(filter.evaluate)
          val query = new Query(sft.getTypeName, filter)
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          result must containTheSameElementsAs(expected)
        }

        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-49, -40, 55, 55, CRS_EPSG_4326)

        Thread.sleep(1000 * 20)

        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, filter)
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          result must beEmpty
        }

        ds.stats.getCount(sft) must beSome(0L)
      } finally {
        RedisSystemProperties.AgeOffInterval.clear()
        ds.removeSchema(sft.getTypeName)
        ds.dispose()
      }
    }

    "expire features based on attribute time" in {

      skipped("Integration tests")

      RedisSystemProperties.AgeOffInterval.set("5 seconds")

      // age off the first feature, since they are one hour apart
      val time = System.currentTimeMillis() + 10000L - features.head.getAttribute("dtg").asInstanceOf[Date].getTime

      val sft = SimpleFeatureTypes.immutable(this.sft, Collections.singletonMap(Configs.FeatureExpiration, s"dtg($time ms)"))
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(sft.getTypeName) must beNull
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        foreach(filters) { filter =>
          val expected = features.filter(filter.evaluate)
          val query = new Query(sft.getTypeName, filter)
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          result must containTheSameElementsAs(expected)
        }

        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-49, -40, 55, 55, CRS_EPSG_4326)

        Thread.sleep(1000 * 20)

        foreach(filters) { filter =>
          val expected = features.drop(1).filter(filter.evaluate)
          val query = new Query(sft.getTypeName, filter)
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          result must containTheSameElementsAs(expected)
        }

        ds.stats.getCount(sft) must beSome(9L)
      } finally {
        RedisSystemProperties.AgeOffInterval.clear()
        ds.removeSchema(sft.getTypeName)
        ds.dispose()
      }
    }
  }
}
