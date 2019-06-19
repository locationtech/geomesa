/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data
import java.util.Collections

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeEach


@RunWith(classOf[JUnitRunner])
class AccumuloFeatureWriterTest extends Specification with TestWithMultipleSfts with BeforeEach {

  sequential

  val spec = "name:String:index=join,age:Integer,dtg:Date,geom:Point:srid=4326"

  lazy val logical = createNewSchema(s"$spec;geomesa.logical.time=true")
  lazy val millis = createNewSchema(s"$spec;geomesa.logical.time=false")

  lazy val sfts = Seq(logical, millis)

  override def before: Any = {
    sfts.foreach{ sft =>
      ds.manager.indices(sft).flatMap(_.getTableNames()).foreach { name =>
        val deleter = connector.createBatchDeleter(name, new Authorizations(), 5, new BatchWriterConfig())
        deleter.setRanges(Collections.singletonList(new org.apache.accumulo.core.data.Range()))
        deleter.delete()
        deleter.close()
      }
    }
  }

  "AccumuloFeatureWriter" should {
    "provide ability to update a single feature that it wrote and preserve feature IDs" in {
      foreach(sfts) { sft =>
        val features = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "fred", 50, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "tom", 60, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "kyle", 2, "2014-01-02", "POINT(45.0 49.0)")
        )

        addFeatures(sft, features)

        val fs = ds.getFeatureSource(sft.getTypeName)
        // turn fred into billy
        fs.modifyFeatures(Array("name", "age"), Array[AnyRef]("billy", Int.box(25)), ECQL.toFilter("name = 'fred'"))

        // delete kyle
        fs.removeFeatures(ECQL.toFilter("name = 'kyle'"))

        val expected = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "billy", 25, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "tom", 60, "2014-01-02", "POINT(45.0 49.0)")
        )

        // read out what we wrote...we should only get tom and billy back out
        val filters = Seq(
          "IN ('fid1', 'fid2', 'fid3')",
          "name IN ('billy', 'tom')",
          "bbox(geom,44,48,46,50)",
          "bbox(geom,44,48,46,50) and dtg during 2014-01-01T23:00:00.000Z/2014-01-02T01:00:00.000Z",
          "dtg during 2014-01-01T23:00:00.000Z/2014-01-02T01:00:00.000Z",
          "age IN (25, 60)"
        )

        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val result = SelfClosingIterator(fs.getFeatures(query).features).toList.sortBy(_.getID)
          result mustEqual expected
        }

        val excludes = Seq(
          "IN ('id3')",
          "name IN ('kyle', 'fred')",
          "age IN (50, 2)"
        )

        foreach(excludes) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val result = SelfClosingIterator(fs.getFeatures(query).features).toList.sortBy(_.getID)
          result must beEmpty
        }
      }
    }

    "be able to write features to a store using a general purpose FeatureWriter" in {
      foreach(sfts) { sft =>
        val writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        val features = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 56, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "george", 33, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "sue", 99, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 50, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 56, "2014-01-02", "POINT(45.0 49.0)")
        )
        features.foreach { f =>
          val writerCreatedFeature = writer.next()
          writerCreatedFeature.setAttributes(f.getAttributes)
          writerCreatedFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          writerCreatedFeature.getUserData.put(Hints.PROVIDED_FID, f.getID)
          writer.write()
        }
        writer.close()

        val query = new Query(sft.getTypeName)
        val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
        result mustEqual features
      }
    }

    "be able to update all features based on some ecql" in {
      foreach(sfts) { sft =>
        val features = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 56, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "george", 33, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "sue", 99, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 50, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 56, "2014-01-02", "POINT(45.0 49.0)")
        )
        addFeatures(sft, features)

        val fs = ds.getFeatureSource(sft.getTypeName)
        fs.modifyFeatures("age", Int.box(60), ECQL.toFilter("(age > 50 AND age < 99) OR (name = 'karen')"))

        val expected = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 60, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 60, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 60, "2014-01-02", "POINT(45.0 49.0)")
        )

        val result = SelfClosingIterator(fs.getFeatures(ECQL.toFilter("age = 60")).features).toList.sortBy(_.getID)
        result mustEqual expected
      }
    }

    "provide ability to remove features" in {
      foreach(sfts) { sft =>
        val features = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 56, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "george", 33, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "sue", 99, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 50, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 56, "2014-01-02", "POINT(45.0 49.0)")
        )
        addFeatures(sft, features)

        WithClose(ds.getFeatureWriter(sft.getTypeName, Filter.INCLUDE, Transaction.AUTO_COMMIT)) { writer =>
          while (writer.hasNext) {
            writer.next()
            writer.remove()
          }
        }

        // ensure that features are deleted
        val filters = Seq(
          "IN ('fid1', 'fid2', 'fid3', 'fid4', 'fid5')",
          "name IN ('will', 'george', 'sue', 'karen', 'bob')",
          "bbox(geom,44,48,46,50)",
          "bbox(geom,44,48,46,50) and dtg during 2014-01-01T23:00:00.000Z/2014-01-02T01:00:00.000Z",
          "dtg during 2014-01-01T23:00:00.000Z/2014-01-02T01:00:00.000Z",
          "age > 30 AND age < 100"
        )

        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
          result must beEmpty
        }

        forall(ds.manager.indices(sft).flatMap(_.getTableNames())) { name =>
          WithClose(connector.createScanner(name, new Authorizations()))(_.iterator.hasNext must beFalse)
        }
      }
    }

    "work with transactions (while ignoring them)" in {
      foreach(sfts) { sft =>
        val features = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 56, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "george", 33, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "sue", 99, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 50, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 56, "2014-01-02", "POINT(45.0 49.0)")
        )

        val c = new DefaultFeatureCollection("0", sft)
        features.foreach { f =>
          f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          c.add(f)
        }

        val fs = ds.getFeatureSource(sft.getTypeName)

        fs.setTransaction(new DefaultTransaction("t1"))
        try {
          fs.addFeatures(c)
          fs.getTransaction.commit()
        } catch {
          case e: Exception =>
            fs.getTransaction.rollback()
            throw e
        } finally {
          fs.getTransaction.close()
          fs.setTransaction(Transaction.AUTO_COMMIT)
        }

        val filters = Seq(
          "IN ('fid1', 'fid2', 'fid3', 'fid4', 'fid5')",
          "name IN ('will', 'george', 'sue', 'karen', 'bob')",
          "bbox(geom,44,48,46,50)",
          "bbox(geom,44,48,46,50) and dtg during 2014-01-01T23:00:00.000Z/2014-01-02T01:00:00.000Z",
          "dtg during 2014-01-01T23:00:00.000Z/2014-01-02T01:00:00.000Z",
          "age > 30 AND age < 100"
        )

        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
          result mustEqual features
        }
      }
    }

    "correctly update keys when indexed values change" in {
      foreach(sfts) { sft =>
        val features = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 56, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "george", 33, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "sue", 99, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 50, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 56, "2014-01-02", "POINT(45.0 49.0)")
        )
        addFeatures(sft, features)

        val update = ECQL.toFilter("name = 'bob' or name = 'karen'")
        WithClose(ds.getFeatureWriter(sft.getTypeName, update, Transaction.AUTO_COMMIT)) { writer =>
          while (writer.hasNext) {
            val feature = writer.next()
            feature.setAttribute("geom", "POINT(50.0 50.0)")
            feature.setAttribute("dtg", "2014-02-02")
            writer.write()
          }
        }

        val expected = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 56, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "george", 33, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "sue", 99, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 50, "2014-02-02", "POINT(50.0 50.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 56, "2014-02-02", "POINT(50.0 50.0)")
        )

        def query(filter: String): Seq[SimpleFeature] = {
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
        }

        // verify old bbox/dtg doesn't return them
        val old = Seq(
          "BBOX(geom,44.9,48.9,45.1,49.1)",
          "dtg DURING 2014-01-01T23:00:00Z/2014-01-02T01:00:00Z",
          "BBOX(geom,44.9,48.9,45.1,49.1) AND dtg DURING 2014-01-01T23:00:00Z/2014-01-02T01:00:00Z"
        )
        foreach(old) { filter =>
          query(filter) mustEqual expected.filter(f => Seq("will", "george", "sue").contains(f.getAttribute("name")))
        }

        // verify new bbox/dtg works
        val updated = Seq(
          "BBOX(geom,49.9,49.9,50.1,50.1)",
          "dtg DURING 2014-02-01T23:00:00Z/2014-02-02T01:00:00Z",
          "BBOX(geom,49.9,49.9,50.1,50.1) AND dtg DURING 2014-02-01T23:00:00Z/2014-02-02T01:00:00Z"
        )
        foreach(updated) { filter =>
          query(filter) mustEqual expected.filter(f => Seq("bob", "karen").contains(f.getAttribute("name")))
        }

        // get them all
        val all = Seq(
          "BBOX(geom,44.0,44.0,51.0,51.0)",
          "dtg DURING 2014-01-01T00:00:00Z/2014-02-03T00:00:00Z",
          "BBOX(geom,44.0,44.0,51.0,51.0) AND dtg DURING 2014-01-01T00:00:00Z/2014-02-03T00:00:00Z"
        )
        foreach(all) { filter =>
          query(filter) mustEqual expected
        }

        // get none
        val none = Seq(
          "BBOX(geom, 30.0,30.0,31.0,31.0)",
          "dtg DURING 2013-12-01T00:00:00Z/2013-12-31T00:00:00Z",
          "BBOX(geom, 30.0,30.0,31.0,31.0) AND dtg DURING 2013-12-01T00:00:00Z/2013-12-31T00:00:00Z"
        )
        foreach(none) { filter =>
          query(filter) must beEmpty
        }
      }
    }

    "delete and re-add the same feature" in {
      foreach(sfts) { sft =>
        val features = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 56, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "george", 33, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "sue", 99, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 50, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 56, "2014-01-02", "POINT(45.0 49.0)")
        )
        addFeatures(sft, features)

        // ensure that features are added
        val filters = Seq(
          "IN ('fid1', 'fid2', 'fid3', 'fid4', 'fid5')",
          "name IN ('will', 'george', 'sue', 'karen', 'bob')",
          "bbox(geom,44,48,46,50)",
          "bbox(geom,44,48,46,50) and dtg during 2014-01-01T23:00:00.000Z/2014-01-02T01:00:00.000Z",
          "dtg during 2014-01-01T23:00:00.000Z/2014-01-02T01:00:00.000Z",
          "age > 30 AND age < 100"
        )

        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
          result mustEqual features
        }

        val delete = ECQL.toFilter("name = 'will'")
        WithClose(ds.getFeatureWriter(sft.getTypeName, delete, Transaction.AUTO_COMMIT)) { writer =>
          while (writer.hasNext) {
            writer.next()
            writer.remove()
          }
        }

        // ensure that will was deleted
        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
          result mustEqual features.drop(1)
        }

        // re-add will
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          val feature = writer.next()
          feature.setAttributes(features.head.getAttributes)
          feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          feature.getUserData.put(Hints.PROVIDED_FID, features.head.getID)
          writer.write()
        }

        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
          result mustEqual features
        }
      }
    }

    "not write partial features" in {
      foreach(sfts) { sft =>
        val invalid = Seq(
          ScalaSimpleFeature.create(sft, "no-geom",  "name", 56, "2016-01-01T00:00:00.000Z", null),
          ScalaSimpleFeature.create(sft, "bad-geom", "name", 56, "2016-01-01T00:00:00.000Z", "POINT(181 0)"),
          ScalaSimpleFeature.create(sft, "bad-date", "name", 56, "2599-01-01T00:00:00.000Z", "POINT(10 10)")
        )
        forall(invalid) { feature =>
          addFeatures(sft, Seq(feature)) must throwAn[IllegalArgumentException]
          forall(ds.manager.indices(sft).flatMap(_.getTableNames())) { table =>
            WithClose(connector.createScanner(table, new Authorizations()))(_.iterator.hasNext must beFalse)
          }
        }
      }
    }

    "create z3 based uuids" in {
      foreach(sfts) { sft =>
        val features = Seq(
          ScalaSimpleFeature.create(sft, "fid1", "will", 56, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid2", "george", 33, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid3", "sue", 99, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid4", "karen", 50, "2014-01-02", "POINT(45.0 49.0)"),
          ScalaSimpleFeature.create(sft, "fid5", "bob", 56, "2014-01-02", "POINT(45.0 49.0)")
        )

        // space out the adding slightly so we ensure they sort how we want - resolution is to the ms
        // also ensure we don't set use_provided_fid
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach { f =>
            FeatureUtils.copyToWriter(writer, f, useProvidedFid = false)
            writer.write()
            writer.flush()
            Thread.sleep(2)
          }
        }

        val query = new Query(sft.getTypeName)
        val ids = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.map(_.getID).sorted
        ids must haveLength(5)
        forall(ids)(_ must not(beMatching("fid\\d")))

        // ensure that the z3 range is the same
        ids.map(_.substring(0, 18)).distinct must haveLength(1)
        // ensure that the second part of the UUID is random
        ids.map(_.substring(19)).distinct must haveLength(5)
      }
    }
  }
}
