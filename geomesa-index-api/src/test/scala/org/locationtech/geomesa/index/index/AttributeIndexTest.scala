/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{Query, Transaction}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.TestGeoMesaDataStore.TestRange
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey}
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.Cardinality
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AttributeIndexTest extends Specification with LazyLogging {

  val typeName = "attr-idx-test"
  val spec =
    "name:String,age:Int,height:Float,dtg:Date,*geom:Point:srid=4326;" +
      "geomesa.indices.enabled='attr:name:geom:dtg,attr:age:geom:dtg,attr:height:geom:dtg'"

  val sft = SimpleFeatureTypes.createType(typeName, spec)

  val aliceGeom   = WKTUtils.read("POINT(45.0 49.0)")
  val billGeom    = WKTUtils.read("POINT(46.0 49.0)")
  val bobGeom     = WKTUtils.read("POINT(47.0 49.0)")
  val charlesGeom = WKTUtils.read("POINT(48.0 49.0)")

  val aliceDate   = Converters.convert("2012-01-01T12:00:00.000Z", classOf[java.util.Date])
  val billDate    = Converters.convert("2013-01-01T12:00:00.000Z", classOf[java.util.Date])
  val bobDate     = Converters.convert("2014-01-01T12:00:00.000Z", classOf[java.util.Date])
  val charlesDate = Converters.convert("2014-01-01T12:30:00.000Z", classOf[java.util.Date])

  val features = Seq(
    Array("alice",   20,   10f, aliceDate,   aliceGeom),
    Array("bill",    21,   11f, billDate,    billGeom),
    Array("bob",     30,   12f, bobDate,     bobGeom),
    Array("charles", null, 12f, charlesDate, charlesGeom)
  ).map { entry =>
    ScalaSimpleFeature.create(sft, entry.head.toString, entry: _*)
  }

  def overlaps(r1: TestRange, r2: TestRange): Boolean = {
    ByteArrays.ByteOrdering.compare(r1.start, r2.start) match {
      case 0 => true
      case i if i < 0 => ByteArrays.ByteOrdering.compare(r1.end, r2.start) > 0
      case i if i > 0 => ByteArrays.ByteOrdering.compare(r2.end, r1.start) > 0
    }
  }

  "AttributeIndex" should {
    "convert shorts to bytes and back" in {
      forall(Seq(0, 32, 127, 128, 129, 255, 256, 257)) { i =>
        val bytes = AttributeIndexKey.indexToBytes(i)
        bytes must haveLength(2)
        val recovered = ByteArrays.readShort(bytes)
        recovered mustEqual i
      }
    }

    "correctly set secondary index ranges" in {
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      def execute(filter: String, explain: Explainer = ExplainNull): Seq[String] = {
        val q = new Query(typeName, ECQL.toFilter(filter))
        // validate that ranges do not overlap
        foreach(ds.getQueryPlan(q, explainer = explain)) { qp =>
          val ranges = qp.ranges.sortBy(_.start)(ByteArrays.ByteOrdering)
          forall(ranges.sliding(2).toSeq) { case Seq(left, right) => overlaps(left, right) must beFalse }
        }
        SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).map(_.getID).toSeq
      }

      // height filter matches bob and charles, st filters only match bob
      // this filter illustrates the overlapping range bug GEOMESA-1902
      val stFilter = "bbox(geom, 46.9, 48.9, 48.1, 49.1) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z"

      // expect z3 ranges with the attribute equals prefix
      val results = execute(s"height = 12.0 AND $stFilter")
      results must haveLength(1)
      results must contain("bob")
    }

    "correctly set secondary index ranges with not nulls" in {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val filter = "contains('POLYGON ((46.9 48.9, 47.1 48.9, 47.1 49.1, 46.9 49.1, 46.9 48.9))', geom) AND " +
          "name = 'bob' AND dtg IS NOT NULL AND name IS NOT NULL AND INCLUDE"
      val q = new Query(sft.getTypeName, ECQL.toFilter(filter))

      ds.getQueryPlan(q).flatMap(_.ranges) must haveLength(sft.getAttributeShards)

      val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).map(_.getID).toList
      results mustEqual Seq("bob")
    }

    "correctly set index ranges without a secondary key" in {
      val spec = "name:String,age:Int,height:Float,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='attr:name'"
      val sft = SimpleFeatureTypes.createType(typeName, spec)

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      ds.manager.indices(sft) must haveLength(1)
      ds.manager.indices(sft).flatMap(_.attributes) mustEqual Seq("name")

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val query = new Query(typeName, ECQL.toFilter("name = 'alice'"))
      val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).map(_.getID).toList

      result mustEqual Seq("alice")
    }

    "handle functions" in {
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val filters = Seq (
        "strToUpperCase(name) = 'BILL'",
        "strCapitalize(name) = 'Bill'",
        "strConcat(name, 'foo') = 'billfoo'",
        "strIndexOf(name, 'ill') = 1",
        "strReplace(name, 'ill', 'all', false) = 'ball'",
        "strSubstring(name, 0, 2) = 'bi'",
        "strToLowerCase(name) = 'bill'",
        "strTrim(name) = 'bill'",
        "abs(age) = 21",
        "ceil(age) = 21",
        "floor(age) = 21",
        "'BILL' = strToUpperCase(name)",
        "strToUpperCase('bill') = strToUpperCase(name)",
        "strToUpperCase(name) = strToUpperCase('bill')",
        "name = strToLowerCase('bill')"
      )
      foreach(filters) { filter =>
        val query = new Query(typeName, ECQL.toFilter(filter))
        SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq mustEqual features.slice(1, 2)
      }
    }

    "handle open-ended secondary filters" in {
      val spec = "dtgStart:Date:default=true,dtgEnd:Date:index=true,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType(typeName, spec)

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      val before = "dtgStart BEFORE 2017-01-02T00:00:00.000Z"
      val after = "dtgEnd AFTER 2017-01-03T00:00:00.000Z"
      val filter = ECQL.toFilter(s"$before AND $after")
      val q = new Query(typeName, filter)
      q.getHints.put(QueryHints.QUERY_INDEX, "attr")

      forall(ds.getQueryPlan(q)) { qp =>
        qp.filter.index must beAnInstanceOf[AttributeIndex]
        qp.filter.primary must beSome(FastFilterFactory.toFilter(sft, after))
        qp.filter.secondary must beSome(FastFilterFactory.toFilter(sft, before))
      }
    }

    "use implicit upper/lower bounds for one-sided secondary filters" in {
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val query = new Query(typeName, ECQL.toFilter("height = 12.0 AND dtg > '2014-01-01T11:45:00.000Z'"))

      foreach(ds.getQueryPlan(query).flatMap(_.ranges)) { range =>
        // verify that we have a z3 suffix...
        // the base length is 10 : 1 (shard) + 2 (i) + 6 (lexicoded float) + 1 (null byte delimiter)
        range.start.length must beGreaterThan(12)
      }

      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).map(_.getID).toList

      results must containTheSameElementsAs(Seq("bob", "charles"))
    }

    "handle various wildcards" in {
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        val bot = ScalaSimpleFeature.copy(features(2))
        bot.setId("bot")
        bot.setAttribute("name", "bot")
        FeatureUtils.write(writer, bot, useProvidedFid = true)
        val bub = ScalaSimpleFeature.copy(features(2))
        bub.setId("bub")
        bub.setAttribute("name", "bub")
        FeatureUtils.write(writer, bub, useProvidedFid = true)
        val bobbed = ScalaSimpleFeature.copy(features(2))
        bobbed.setId("bobbed")
        bobbed.setAttribute("name", "bobbed")
        FeatureUtils.write(writer, bobbed, useProvidedFid = true)
      }

      val queries = Seq(
        "name like 'alice'" -> Seq("alice"),
        "name like 'b%'"    -> Seq("bill", "bob", "bobbed", "bot", "bub"),
        "name like 'bo_'"   -> Seq("bob", "bot"),
        "name like 'b_b'"   -> Seq("bob", "bub"),
        "name like 'b%b'"   -> Seq("bob", "bub"),
        "name like 'b__l'"  -> Seq("bill"),
        "name ilike 'B%b'"  -> Seq("bob", "bub"),
        "name ilike 'ALi%'" -> Seq("alice")
      )
      val withDates = queries.map { case (filter, expected) =>
        s"$filter AND dtg > '2012-01-01T11:45:00.000Z' AND dtg < '2014-01-01T13:00:00.000Z'" -> expected
      }
      foreach(queries ++ withDates) { case (filter, expected) =>
        val query = new Query(typeName, ECQL.toFilter(filter))
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).map(_.getID).toList
        results must containTheSameElementsAs(expected)
      }
    }

    "handle large or'd attribute queries" in {
      // test against the attr+date tiered index, otherwise secondary z3 ranges slow everything down
      val spec = "attr:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='z3,attr:8:attr:dtg'"
      val sft = SimpleFeatureTypes.createType(typeName, spec)

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      val r = new Random(0L)

      val numFeatures = 5000
      val features = (0 until numFeatures).map { i =>
        val a = (0 until 20).map(_ => r.nextInt(9).toString).mkString + "<foobar>"
        val day = i % 30
        val values = Array[AnyRef](a, f"2014-01-$day%02dT01:00:00.000Z", WKTUtils.read(s"POINT(45.0 45)") )
        val sf = new ScalaSimpleFeature(sft, i.toString, values)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf
      }

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val dtgPart = "dtg between '2014-01-01T00:00:00.000Z' and '2014-01-31T00:00:00.000Z'"
      val attrPart = "attr in (" + features.take(1000).map(_.getAttribute(0)).map(a => s"'$a'").mkString(", ") + ")"
      val query = new Query(sft.getTypeName, ECQL.toFilter(s"$dtgPart and $attrPart"))

      query.getHints.put(QueryHints.QUERY_INDEX, "attr")

      val start = System.currentTimeMillis()

      val feats = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      while (feats.hasNext) {
        feats.next()
      }

      val time = System.currentTimeMillis() - start

      // set the check fairly high so that we don't get random test failures, but log a warning
      if (time > 500L) {
        logger.warn(s"Attribute query processing took ${time}ms for large OR query")
      }
      time must beLessThan(10000L)
    }

    "de-prioritize not-null queries" in {
      import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

      val spec = "name:String:index=true:cardinality=high,age:Int:index=true,height:Float:index=true," +
          "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType(typeName, spec)

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      ds.getSchema(typeName).getDescriptor("name").getCardinality mustEqual Cardinality.HIGH

      val notNull = FastFilterFactory.toFilter(sft, "name IS NOT NULL")
      val notNullPlans = ds.getQueryPlan(new Query(typeName, notNull))
      notNullPlans must haveLength(1)
      notNullPlans.head.filter.index must beAnInstanceOf[AttributeIndex]
      notNullPlans.head.filter.primary must beSome(notNull)
      notNullPlans.head.filter.secondary must beNone

      val agePlans = ds.getQueryPlan(new Query(typeName, ECQL.toFilter("age = 21 AND name IS NOT NULL")))
      agePlans must haveLength(1)
      agePlans.head.filter.index must beAnInstanceOf[AttributeIndex]
      agePlans.head.filter.primary must beSome(FastFilterFactory.toFilter(sft, "age = 21"))
      agePlans.head.filter.secondary must beSome(notNull)
    }

    "handle secondary date equality filters" in {
      val spec = "name:String,age:Int,height:Float,dtg:Date,*geom:Point:srid=4326;" +
          "geomesa.indices.enabled='attr:name:dtg'"
      val sft = SimpleFeatureTypes.createType(typeName, spec)
      val features = this.features.map(ScalaSimpleFeature.copy(sft, _))

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val filters = Seq(
        "dtg = '2014-01-01T12:00:00.000Z'",
        "dtg tequals 2014-01-01T12:00:00.000Z",
        "dtg during 2014-01-01T11:59:59.999Z/2014-01-01T12:00:00.001Z",
        "dtg between '2014-01-01T12:00:00.000Z' and '2014-01-01T12:00:00.000Z'",
        "dtg >= '2014-01-01T12:00:00.000Z' and dtg < '2014-01-01T12:00:00.001Z'"
      )
      foreach(filters) { filter =>
        val query = new Query(typeName, ECQL.toFilter(s"name = 'bob' and $filter"))
        SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList mustEqual
            features.slice(2, 3)
      }
    }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 0d3cbc99a2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> aa627812da (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 1690f5dad4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 54fb5ef951 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9b796685be (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 914ec419fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 9b796685be (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 76328bb492 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 1690f5dad (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 54fb5ef95 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

    "handle filter.EXCLUDE with query hint" in {
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val query = new Query(typeName, Filter.EXCLUDE)
      query.getHints.put(QueryHints.QUERY_INDEX, "attr")

      foreach(ds.getQueryPlan(query))(_.ranges must beEmpty)

      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).map(_.getID).toList

      results must beEmpty
    }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> aa627812da (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 1690f5dad4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 914ec419fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9b796685be (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 54fb5ef95 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54fb5ef951 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 914ec419fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b796685be (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 76328bb492 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 1690f5dad (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 54fb5ef95 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
  }
}
