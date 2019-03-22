/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.{Collections, Date}

import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.NameImpl
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{EmptyPlan, JoinPlan}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.TestData
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.conf.{QueryHints, QueryProperties}
import org.locationtech.geomesa.index.index.NamedIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.index.utils.{ExplainNull, ExplainString}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodedValues
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Coordinate
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreQueryTest extends Specification with TestWithMultipleSfts {

  import org.locationtech.geomesa.filter.ff

  sequential

  val defaultSft = createNewSchema("name:String:index=join,geom:Point:srid=4326,dtg:Date")
  addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-1", "name1", "POINT(45 49)", "2010-05-07T12:30:00.000Z"))

  "AccumuloDataStore" should {
    "return an empty iterator correctly" in {
      val fs = ds.getFeatureSource(defaultSft.getTypeName)

      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val cqlFilter = CQL.toFilter(s"BBOX(geom, 64.9,68.9,65.1,69.1)")
      val query = new Query(defaultSft.getTypeName, cqlFilter)

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features

      "where schema matches" >> { results.getSchema mustEqual defaultSft }
      "and there are no results" >> { features.hasNext must beFalse }
    }

    "process an exclude query correctly" in {
      val fs = ds.getFeatureSource(defaultSft.getTypeName)

      val query = new Query(defaultSft.getTypeName, Filter.EXCLUDE)

      ds.getQueryPlan(query) must beEmpty

      val features = fs.getFeatures(query).features
      try {
        features.hasNext must beFalse
      } finally {
        features.close()
      }
    }

    "process a DWithin query correctly" in {
      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val geomFactory = JTSFactoryFinder.getGeometryFactory
      val q = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate(45.000001, 48.99999))), 100.0, "meters")
      val query = new Query(defaultSft.getTypeName, q)

      // Let's read out what we wrote.
      val results = ds.getFeatureSource(defaultSft.getTypeName).getFeatures(query)
      val features = results.features

      "with correct result" >> {
        features.hasNext must beTrue
        features.next().getID mustEqual "fid-1"
        features.hasNext must beFalse
      }
    }

    "process a DWithin of a Linestring and dtg During query correctly" >> {
      val lineOfBufferCoords: Array[Coordinate] = Array(new Coordinate(-45, 0), new Coordinate(-90, 45))
      val geomFactory = JTSFactoryFinder.getGeometryFactory

      // create the data store
      val sftPoints = createNewSchema("*geom:Point:srid=4326,dtg:Date")

      // add the 150 excluded points
      TestData.excludedDwithinPoints.zipWithIndex.foreach{ case (p, i) =>
        addFeature(sftPoints, ScalaSimpleFeature.create(sftPoints, s"exfid$i", p, "2014-06-07T12:00:00.000Z"))
      }

      // add the 50 included points
      TestData.includedDwithinPoints.zipWithIndex.foreach{ case (p, i) =>
        addFeature(sftPoints, ScalaSimpleFeature.create(sftPoints, s"infid$i", p, "2014-06-07T12:00:00.000Z"))
      }

      // compose the query
      val during = ECQL.toFilter("dtg DURING 2014-06-07T11:00:00.000Z/2014-06-07T13:00:00.000Z")

      "with correct result when using a dwithin of degrees" >> {
        val dwithinUsingDegrees = ff.dwithin(ff.property("geom"),
          ff.literal(geomFactory.createLineString(lineOfBufferCoords)), 1.0, "degrees")
        val filterUsingDegrees  = ff.and(during, dwithinUsingDegrees)
        val queryUsingDegrees   = new Query(sftPoints.getTypeName, filterUsingDegrees)
        val resultsUsingDegrees = ds.getFeatureSource(sftPoints.getTypeName).getFeatures(queryUsingDegrees)
        SelfClosingIterator(resultsUsingDegrees.features).toSeq must haveLength(50)
      }.pendingUntilFixed("Fixed Z3 'During And Dwithin' queries for a buffer created with unit degrees")

      "with correct result when using a dwithin of meters" >> {
        val dwithinUsingMeters = ff.dwithin(ff.property("geom"),
          ff.literal(geomFactory.createLineString(lineOfBufferCoords)), 150000, "meters")
        val filterUsingMeters  = ff.and(during, dwithinUsingMeters)
        val queryUsingMeters   = new Query(sftPoints.getTypeName, filterUsingMeters)
        val resultsUsingMeters = ds.getFeatureSource(sftPoints.getTypeName).getFeatures(queryUsingMeters)
        SelfClosingIterator(resultsUsingMeters.features).toSeq must haveLength(50)
      }
    }

    "handle bboxes without property name" in {
      val filterNull = ff.bbox(ff.property(null.asInstanceOf[String]), 40, 44, 50, 54, "EPSG:4326")
      val filterEmpty = ff.bbox(ff.property(""), 40, 44, 50, 54, "EPSG:4326")
      val queryNull = new Query(defaultSft.getTypeName, filterNull)
      val queryEmpty = new Query(defaultSft.getTypeName, filterEmpty)

      val planNull = ds.getQueryPlan(queryNull)
      val planEmpty = ds.getQueryPlan(queryEmpty)

      planNull must haveLength(1)
      planNull.head.filter.index.name mustEqual Z2Index.name

      planEmpty must haveLength(1)
      planNull.head.filter.index.name mustEqual Z2Index.name

      val featuresNull = SelfClosingIterator(ds.getFeatureSource(defaultSft.getTypeName).getFeatures(queryNull).features).toSeq
      val featuresEmpty = SelfClosingIterator(ds.getFeatureSource(defaultSft.getTypeName).getFeatures(queryEmpty).features).toSeq

      featuresNull.map(_.getID) mustEqual Seq("fid-1")
      featuresEmpty.map(_.getID) mustEqual Seq("fid-1")
    }

    "handle out-of-world bboxes" >> {
      val sft = createNewSchema("name:String,*geom:Point:srid=4326", None)
      val typeName = sft.getTypeName
      val feature = ScalaSimpleFeature.create(sft, "1", "name1", "POINT (-100.236523 23)")
      addFeature(sft, feature)
      // example from geoserver open-layers preview
      val ecql = "BBOX(geom, 254.17968736588955,16.52343763411045,264.02343736588955,26.36718763411045) OR " +
          "BBOX(geom, -105.82031263411045,16.52343763411045,-95.97656263411045,26.36718763411045)"
      val fs = ds.getFeatureSource(typeName)
      val result = SelfClosingIterator(fs.getFeatures(new Query(typeName, ECQL.toFilter(ecql))).features).toList
      result must haveLength(1)
      result.head mustEqual feature
    }

    "process an OR query correctly obeying inclusion-exclusion principle" >> {
      val sft = createNewSchema("name:String,geom:Point:srid=4326,dtg:Date")

      val randVal: (Double, Double) => Double = {
        val r = new Random(System.nanoTime())
        (low, high) => {
          (r.nextDouble() * (high - low)) + low
        }
      }
      val features = (0 until 1000).map { i =>
        val lat = randVal(-0.001, 0.001)
        val lon = randVal(-0.001, 0.001)
        ScalaSimpleFeature.create(sft, s"fid-$i", "testType", s"POINT($lat $lon)")
      }
      addFeatures(sft, features)

      val fs = ds.getFeatureSource(sft.getTypeName)

      val geomFactory = JTSFactoryFinder.getGeometryFactory
      val urq = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate( 0.0005,  0.0005))), 150.0, "meters")
      val llq = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate(-0.0005, -0.0005))), 150.0, "meters")
      val orq = ff.or(urq, llq)
      val andq = ff.and(urq, llq)
      val urQuery  = new Query(sft.getTypeName,  urq)
      val llQuery  = new Query(sft.getTypeName,  llq)
      val orQuery  = new Query(sft.getTypeName,  orq)
      val andQuery = new Query(sft.getTypeName, andq)

      val urNum  = SelfClosingIterator(fs.getFeatures(urQuery).features).length
      val llNum  = SelfClosingIterator(fs.getFeatures(llQuery).features).length
      val orNum  = SelfClosingIterator(fs.getFeatures(orQuery).features).length
      val andNum = SelfClosingIterator(fs.getFeatures(andQuery).features).length

      (urNum + llNum) mustEqual (orNum + andNum)
    }

    "process 'exists' queries correctly" in {
      val fs = ds.getFeatureSource(defaultSft.getTypeName)

      val exists = ECQL.toFilter("name EXISTS")
      val doesNotExist = ECQL.toFilter("name DOES-NOT-EXIST")

      val existsResults =
        SelfClosingIterator(fs.getFeatures(new Query(defaultSft.getTypeName, exists)).features).toList
      val doesNotExistResults =
        SelfClosingIterator(fs.getFeatures(new Query(defaultSft.getTypeName, doesNotExist)).features).toList

      existsResults must haveLength(1)
      existsResults.head.getID mustEqual "fid-1"
      doesNotExistResults must beEmpty
    }

    "handle between intra-day queries" in {
      val filter =
        CQL.toFilter("bbox(geom,40,40,60,60) AND dtg BETWEEN '2010-05-07T12:00:00.000Z' AND '2010-05-07T13:00:00.000Z'")
      val query = new Query(defaultSft.getTypeName, filter)
      val features = SelfClosingIterator(ds.getFeatureSource(defaultSft.getTypeName).getFeatures(query).features).toList
      features.map(DataUtilities.encodeFeature) mustEqual List("fid-1=name1|POINT (45 49)|2010-05-07T12:30:00.000Z")
    }

    "handle 1s duration queries" in {
      val filter = CQL.toFilter("bbox(geom,40,40,60,60) AND dtg DURING 2010-05-07T12:30:00.000Z/T1S")
      val query = new Query(defaultSft.getTypeName, filter)
      val features = SelfClosingIterator(ds.getFeatureSource(defaultSft.getTypeName).getFeatures(query).features).toList
      features.map(DataUtilities.encodeFeature) mustEqual List("fid-1=name1|POINT (45 49)|2010-05-07T12:30:00.000Z")
    }

    "handle large ranges" in {
      skipped("takes ~10 seconds")
      val filter = ECQL.toFilter("contains(POLYGON ((40 40, 50 40, 50 50, 40 50, 40 40)), geom) AND " +
          "dtg BETWEEN '2010-01-01T00:00:00.000Z' AND '2010-12-31T23:59:59.000Z'")
      val query = new Query(defaultSft.getTypeName, filter)
      val features = SelfClosingIterator(ds.getFeatureSource(defaultSft.getTypeName).getFeatures(query).features).toList
      features.map(DataUtilities.encodeFeature) mustEqual List("fid-1=name1|POINT (45 49)|2010-05-07T12:30:00.000Z")
    }

    "handle out-of-bound longitude and in-bounds latitude bboxes" in {
      val filter = ECQL.toFilter("BBOX(geom, -266.8359375,-75.5859375,279.4921875,162.7734375)")
      val query = new Query(defaultSft.getTypeName, filter)
      val features = SelfClosingIterator(ds.getFeatureSource(defaultSft.getTypeName).getFeatures(query).features).toList
      features.map(DataUtilities.encodeFeature) mustEqual List("fid-1=name1|POINT (45 49)|2010-05-07T12:30:00.000Z")
    }

    "handle requests with namespaces" in {
      import AccumuloDataStoreParams.NamespaceParam

      import scala.collection.JavaConversions._

      val ns = "mytestns"
      val typeName = "namespacetest"

      val sft = SimpleFeatureTypes.createType(typeName, "name:String,geom:Point:srid=4326")
      val sftWithNs = SimpleFeatureTypes.createType(ns, typeName, "name:String,geom:Point:srid=4326")

      ds.createSchema(sftWithNs)

      ds.getSchema(typeName) mustEqual SimpleFeatureTypes.immutable(sft)
      ds.getSchema(new NameImpl(ns, typeName)) mustEqual SimpleFeatureTypes.immutable(sft)

      val dsWithNs = DataStoreFinder.getDataStore(dsParams ++ Map(NamespaceParam.key -> "ns0"))
      val name = dsWithNs.getSchema(typeName).getName
      name.getNamespaceURI mustEqual "ns0"
      name.getLocalPart mustEqual typeName

      val sf = ScalaSimpleFeature.create(sft, "fid-1", "name1", "POINT(45 49)")
      addFeature(sft, sf)

      val queries = Seq(
        new Query(typeName),
        new Query(typeName, ECQL.toFilter("bbox(geom,40,45,50,55)")),
        new Query(typeName, Filter.INCLUDE, Array("geom")),
        new Query(typeName, ECQL.toFilter("bbox(geom,40,45,50,55)"), Array("geom"))
      )
      foreach(queries) { query =>
        val reader = dsWithNs.getFeatureReader(query, Transaction.AUTO_COMMIT)
        reader.getFeatureType.getName mustEqual name
        val features = SelfClosingIterator(reader).toList
        features.map(_.getID) mustEqual Seq(sf.getID)
        features.head.getFeatureType.getName mustEqual name
      }
    }

    "handle cql functions" in {
      val sftName = defaultSft.getTypeName
      val filters = Seq("name = 'name1'", "IN('fid-1')", "bbox(geom, 44, 48, 46, 50)",
        "bbox(geom, 44, 48, 46, 50) AND dtg DURING 2010-05-07T12:00:00.000Z/2010-05-07T13:00:00.000Z")
      val positives = filters.map(f => new Query(sftName, ECQL.toFilter(s"$f AND geometryType(geom) = 'Point'")))
      val negatives = filters.map(f => new Query(sftName, ECQL.toFilter(s"$f AND geometryType(geom) = 'Polygon'")))

      val pStrategies = positives.map(ds.getQueryPlan(_))
      val nStrategies = negatives.map(ds.getQueryPlan(_))

      forall(pStrategies ++ nStrategies)(_ must haveLength(1))
      pStrategies.map(_.head.filter.index.name) mustEqual Seq(JoinIndex, IdIndex, Z2Index, Z3Index).map(_.name)
      nStrategies.map(_.head.filter.index.name) mustEqual Seq(JoinIndex, IdIndex, Z2Index, Z3Index).map(_.name)

      forall(positives) { query =>
        val result = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
        result must haveLength(1)
        result.head.getID mustEqual "fid-1"
      }
      forall(negatives) { query =>
        val result = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
        result must beEmpty
      }
    }

    "handle ANDed Filter.INCLUDE" in {
      val filter = ff.and(Filter.INCLUDE,
        ECQL.toFilter("dtg DURING 2010-05-07T12:00:00.000Z/2010-05-07T13:00:00.000Z and bbox(geom,40,44,50,54)"))
      val reader = ds.getFeatureReader(new Query(defaultSft.getTypeName, filter), Transaction.AUTO_COMMIT)
      val features = SelfClosingIterator(reader).toList
      features must haveLength(1)
      features.head.getID mustEqual "fid-1"
    }

    "short-circuit disjoint geometry predicates" in {
      val filter = ECQL.toFilter("bbox(geom,0,0,10,10) AND bbox(geom,20,20,30,30)")
      val query = new Query(defaultSft.getTypeName, filter)
      val plans = ds.getQueryPlan(query)
      plans must haveLength(1)
      plans.head must beAnInstanceOf[EmptyPlan]
      val reader = ds.getFeatureReader(new Query(defaultSft.getTypeName, filter), Transaction.AUTO_COMMIT)
      val features = SelfClosingIterator(reader).toList
      features must beEmpty
    }

    "short-circuit disjoint date predicates" in {
      val filter = ECQL.toFilter("dtg DURING 2010-05-07T12:00:00.000Z/2010-05-07T13:00:00.000Z AND " +
          "dtg DURING 2010-05-07T15:00:00.000Z/2010-05-07T17:00:00.000Z AND bbox(geom,0,0,10,10)")
      val query = new Query(defaultSft.getTypeName, filter)
      val plans = ds.getQueryPlan(query)
      plans must haveLength(1)
      plans.head must beAnInstanceOf[EmptyPlan]
      val reader = ds.getFeatureReader(new Query(defaultSft.getTypeName, filter), Transaction.AUTO_COMMIT)
      val features = SelfClosingIterator(reader).toList
      features must beEmpty
    }

    "support multi-polygon and bbox predicates" in {
      val bbox = "bbox(geom,44.9,48.9,45.1,49.1)"
      val intersects = "intersects(geom, 'MULTIPOLYGON (((40 40, 55 60, 20 60, 40 40)),((25 15, 50 20, 20 30, 15 20, 25 15)))')"
      val dtg = "dtg DURING 2010-05-07T12:29:50.000Z/2010-05-07T12:30:10.000Z"
      val dtg2 = "dtg DURING 2010-05-07T12:00:00.000Z/2010-05-07T13:00:00.000Z"
      val queries = Seq(s"$bbox AND $dtg AND $intersects AND $dtg2", s"$intersects AND $dtg2 AND $bbox AND $dtg")
      forall(queries) { ecql =>
        val query = new Query(defaultSft.getTypeName, ECQL.toFilter(ecql))
        val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
        val features = SelfClosingIterator(reader).toList
        features must haveLength(1)
        features.head.getID mustEqual "fid-1"
      }
    }

    "support complex OR queries" in {
      val sft = createNewSchema("attr1:String,attr2:Double,dtg:Date,*geom:Point:srid=4326")
      val date = "dtg > '2010-05-07T12:29:50.000Z' AND dtg < '2010-05-07T12:30:10.000Z'"
      def attr(fuzz: Int) = s"(intersects(geom, POLYGON ((39.$fuzz 39.$fuzz, 44.$fuzz 39.$fuzz, 44.$fuzz 44.$fuzz, 39.$fuzz 44.$fuzz, 39.$fuzz 39.$fuzz))) AND attr1 like 'foo%' AND attr2 > 0.001 and attr2 < 2.001)"
      val disjoint = "disjoint(geom, POLYGON ((40 40, 42 42, 42 44, 40 40)))"
      val clauses = (0 until 128).map(attr).mkString(" OR ")
      val filter = s"$date AND ($clauses) AND $disjoint"
      val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
      val start = System.currentTimeMillis()
      // makes sure this doesn't blow up
      SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      // we give it 30 seconds due to weak build boxes
      (System.currentTimeMillis() - start) must beLessThan(30000L)
    }

    "support bin queries" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val sft = createNewSchema(s"name:String,dtg:Date,*geom:Point:srid=4326")

      addFeature(sft, ScalaSimpleFeature.create(sft, "1", "name1", "2010-05-07T00:00:00.000Z", "POINT(45 45)"))
      addFeature(sft, ScalaSimpleFeature.create(sft, "2", "name2", "2010-05-07T01:00:00.000Z", "POINT(45 45)"))

      val query = new Query(sft.getTypeName, ECQL.toFilter("BBOX(geom,40,40,50,50)"))
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      query.getHints.put(QUERY_INDEX, Z2Index.name)
      val queryPlanner = new QueryPlanner(ds)
      val results = queryPlanner.runQuery(sft, query, ExplainNull).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toSeq
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveSize(2)
      bins.map(_.trackId) must containAllOf(Seq("name1", "name2").map(_.hashCode))
    }

    "support bin queries with linestrings" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val sft = createNewSchema(s"name:String,dtgs:List[Date],dtg:Date,*geom:LineString:srid=4326")
      val dtgs1 = new java.util.ArrayList[Date]
      dtgs1.add(Converters.convert("2010-05-07T00:00:00.000Z", classOf[Date]))
      dtgs1.add(Converters.convert("2010-05-07T00:01:00.000Z", classOf[Date]))
      dtgs1.add(Converters.convert("2010-05-07T00:02:00.000Z", classOf[Date]))
      dtgs1.add(Converters.convert("2010-05-07T00:03:00.000Z", classOf[Date]))
      val dtgs2 = new java.util.ArrayList[Date]
      dtgs2.add(Converters.convert("2010-05-07T01:00:00.000Z", classOf[Date]))
      dtgs2.add(Converters.convert("2010-05-07T01:01:00.000Z", classOf[Date]))
      dtgs2.add(Converters.convert("2010-05-07T01:02:00.000Z", classOf[Date]))
      addFeature(sft, ScalaSimpleFeature.create(sft, "1", "name1", dtgs1, "2010-05-07T00:00:00.000Z", "LINESTRING(40 41, 42 43, 44 45, 46 47)"))
      addFeature(sft, ScalaSimpleFeature.create(sft, "2", "name2", dtgs2, "2010-05-07T01:00:00.000Z", "LINESTRING(50 50, 51 51, 52 52)"))

      forall(Seq(2, 1000)) { batch =>
        val query = new Query(sft.getTypeName, ECQL.toFilter("BBOX(geom,40,40,55,55)"))
        query.getHints.put(BIN_TRACK, "name")
        query.getHints.put(BIN_BATCH_SIZE, batch)
        query.getHints.put(BIN_DTG, "dtgs")

        val bytes = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
        forall(bytes)(_ must beAnInstanceOf[Array[Byte]])
        val bins = bytes.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
        bins must haveSize(7)
        val sorted = bins.sortBy(_.dtg)
        sorted(0) mustEqual EncodedValues("name1".hashCode, 41, 40, dtgs1(0).getTime, -1L)
        sorted(1) mustEqual EncodedValues("name1".hashCode, 43, 42, dtgs1(1).getTime, -1L)
        sorted(2) mustEqual EncodedValues("name1".hashCode, 45, 44, dtgs1(2).getTime, -1L)
        sorted(3) mustEqual EncodedValues("name1".hashCode, 47, 46, dtgs1(3).getTime, -1L)
        sorted(4) mustEqual EncodedValues("name2".hashCode, 50, 50, dtgs2(0).getTime, -1L)
        sorted(5) mustEqual EncodedValues("name2".hashCode, 51, 51, dtgs2(1).getTime, -1L)
        sorted(6) mustEqual EncodedValues("name2".hashCode, 52, 52, dtgs2(2).getTime, -1L)
      }
    }

    "support IN queries without dtg on non-indexed string attributes" in {
      val sft = createNewSchema(s"name:String,dtg:Date,*geom:Point:srid=4326")

      addFeature(sft, ScalaSimpleFeature.create(sft, "1", "name1", "2010-05-07T00:00:00.000Z", "POINT(45 45)"))
      addFeature(sft, ScalaSimpleFeature.create(sft, "2", "name2", "2010-05-07T01:00:00.000Z", "POINT(45 46)"))

      val filter = ECQL.toFilter("name IN('name1','name2') AND BBOX(geom, 40.0,40.0,50.0,50.0)")
      val query = new Query(sft.getTypeName, filter)
      val features = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).toList
      features.map(DataUtilities.encodeFeature) must containTheSameElementsAs {
        List("1=name1|2010-05-07T00:00:00.000Z|POINT (45 45)", "2=name2|2010-05-07T01:00:00.000Z|POINT (45 46)")
      }
    }

    "support IN queries without dtg on indexed string attributes" in {
      val sft = createNewSchema("name:String:index=join,dtg:Date,*geom:Point:srid=4326")

      addFeature(sft, ScalaSimpleFeature.create(sft, "1", "name1", "2010-05-07T00:00:00.000Z", "POINT(45 45)"))
      addFeature(sft, ScalaSimpleFeature.create(sft, "2", "name2", "2010-05-07T01:00:00.000Z", "POINT(45 46)"))

      val filter = ECQL.toFilter("name IN('name1','name2') AND BBOX(geom, -180.0,-90.0,180.0,90.0)")
      val query = new Query(sft.getTypeName, filter)
      val features = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).toList
      features.map(DataUtilities.encodeFeature).sorted mustEqual List("1=name1|2010-05-07T00:00:00.000Z|POINT (45 45)", "2=name2|2010-05-07T01:00:00.000Z|POINT (45 46)").sorted
    }

    "kill queries after a configurable timeout" in {
      import scala.concurrent.duration._

      val params = dsParams ++ Map(AccumuloDataStoreParams.QueryTimeoutParam.getName -> "1s")

      val dsWithTimeout = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
      val reader = dsWithTimeout.getFeatureReader(new Query(defaultSft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT)
      reader.isClosed must beFalse
      eventually(20, 200.millis)(reader.isClosed must beTrue)
    }

    "block full table scans" in {
      val sft = createNewSchema("name:String:index=join,age:Int,geom:Point:srid=4326,dtg:Date")
      val feature = ScalaSimpleFeature.create(sft, "fid-1", "name1", "23", "POINT(45 49)", "2010-05-07T12:30:00.000Z")
      addFeature(sft, feature)

      val filters = Seq(
        "IN ('fid-1')",
        "name = 'name1'",
        "name IN ('name1', 'name2')",
        "bbox(geom,44,48,46,50)",
        "bbox(geom,44,48,46,50) AND age < 25",
        "dtg during 2010-05-07T12:25:00.000Z/2010-05-07T12:35:00.000Z",
        "bbox(geom,44,48,46,50) AND dtg during 2010-05-07T12:25:00.000Z/2010-05-07T12:35:00.000Z  AND age = 23"
      )
      val fullScans = Seq("INCLUDE", "age = 23")

      // test that blocking full table scans doesn't interfere with regular queries
      QueryProperties.BlockFullTableScans.threadLocalValue.set("true")
      try {
        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val features = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).toList
          features mustEqual List(feature)
        }
        foreach(fullScans) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          ds.getFeatureSource(sft.getTypeName).getFeatures(query).features must throwA[RuntimeException]
        }
        // verify that we won't block if max features is set
        foreach(fullScans) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter), 10, null: Array[String], null)
          val features = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).toList
          features mustEqual List(feature)
        }

        // verify that we can override individually
        System.setProperty(s"geomesa.scan.${sft.getTypeName}.block-full-table", "false")
        foreach(fullScans) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          val features = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).toList
          features mustEqual List(feature)
        }
        // verify that we can also block individually
        QueryProperties.BlockFullTableScans.threadLocalValue.remove()
        System.setProperty(s"geomesa.scan.${sft.getTypeName}.block-full-table", "true")
        foreach(fullScans) { filter =>
          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          ds.getFeatureSource(sft.getTypeName).getFeatures(query).features must throwA[RuntimeException]
        }
      } finally {
        QueryProperties.BlockFullTableScans.threadLocalValue.remove()
        System.clearProperty(s"geomesa.scan.${sft.getTypeName}.block-full-table")
      }
    }

    "allow query strategy to be specified via view params" in {
      val filter = "BBOX(geom,40,40,50,50) and dtg during 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z and name='name1'"
      val query = new Query(defaultSft.getTypeName, ECQL.toFilter(filter))

      def expectStrategy(strategy: NamedIndex) = {
        val plans = ds.getQueryPlan(query)
        plans must haveLength(1)
        plans.head.filter.index.name mustEqual strategy.name
        val res = SelfClosingIterator(ds.getFeatureSource(defaultSft.getTypeName).getFeatures(query).features).map(_.getID).toList
        res must containTheSameElementsAs(Seq("fid-1"))
      }

      forall(Seq(JoinIndex, Z2Index, Z3Index, IdIndex)) { index =>
        val idx = ds.manager.indices(defaultSft).find(_.name == index.name).orNull
        idx must not(beNull)
        query.getHints.put(QUERY_INDEX, idx.identifier)
        expectStrategy(index)
        query.getHints.remove(QUERY_INDEX)
        query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, Collections.singletonMap("QUERY_INDEX", index.name))
        expectStrategy(index)
        query.getHints.remove(QUERY_INDEX)
        query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, Collections.singletonMap("STRATEGY", index.name))
        expectStrategy(index)
      }
    }

    "allow for loose bounding box config" >> {

      val bbox = "bbox(geom,45.000000001,49.000000001,46,50)"
      val z2Query = new Query(defaultSft.getTypeName, ECQL.toFilter(bbox))
      val z3Query = new Query(defaultSft.getTypeName,
        ECQL.toFilter(s"$bbox AND dtg DURING 2010-05-07T12:25:00.000Z/2010-05-07T12:35:00.000Z"))

      val params = dsParams ++ Map(AccumuloDataStoreParams.LooseBBoxParam.getName -> "false")

      val strictDs = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

      "with loose bbox as default" >> {
        "for z2 index" >> {
          val looseReader = ds.getFeatureReader(z2Query, Transaction.AUTO_COMMIT)
          try {
            looseReader.hasNext must beTrue
          } finally {
            looseReader.close()
          }
        }
        "for z3 index" >> {
          val looseReader = ds.getFeatureReader(z3Query, Transaction.AUTO_COMMIT)
          try {
            looseReader.hasNext must beTrue
          } finally {
            looseReader.close()
          }
        }
      }

      "with strict configuration through data store params" >> {
        "for z2 index" >> {
          val strictReader = strictDs.getFeatureReader(z2Query, Transaction.AUTO_COMMIT)
          try {
            strictReader.hasNext must beFalse
          } finally {
            strictReader.close()
          }
        }
        "for z3 index" >> {
          val strictReader = strictDs.getFeatureReader(z3Query, Transaction.AUTO_COMMIT)
          try {
            strictReader.hasNext must beFalse
          } finally {
            strictReader.close()
          }
        }
      }

      "with query hints" >> {
        "overriding loose config" >> {
          "for z2 index" >> {
            val strictZ2Query = new Query(z2Query)
            strictZ2Query.getHints.put(QueryHints.LOOSE_BBOX, java.lang.Boolean.FALSE)
            val strictReader = ds.getFeatureReader(strictZ2Query, Transaction.AUTO_COMMIT)
            try {
              strictReader.hasNext must beFalse
            } finally {
              strictReader.close()
            }
          }
          "for z3 index" >> {
            val strictZ3Query = new Query(z3Query)
            strictZ3Query.getHints.put(QueryHints.LOOSE_BBOX, java.lang.Boolean.FALSE)
            val strictReader = ds.getFeatureReader(strictZ3Query, Transaction.AUTO_COMMIT)
            try {
              strictReader.hasNext must beFalse
            } finally {
              strictReader.close()
            }
          }
        }

        "overriding strict config" >> {
          "for z2 index" >> {
            val looseZ2Query = new Query(z2Query)
            looseZ2Query.getHints.put(QueryHints.LOOSE_BBOX, java.lang.Boolean.TRUE)
            val looseReader = strictDs.getFeatureReader(looseZ2Query, Transaction.AUTO_COMMIT)
            try {
              looseReader.hasNext must beTrue
            } finally {
              looseReader.close()
            }
          }

          "for z3 index" >> {
            val looseZ3Query = new Query(z3Query)
            looseZ3Query.getHints.put(QueryHints.LOOSE_BBOX, java.lang.Boolean.TRUE)
            val looseReader = strictDs.getFeatureReader(looseZ3Query, Transaction.AUTO_COMMIT)
            try {
              looseReader.hasNext must beTrue
            } finally {
              looseReader.close()
            }
          }
        }
      }

      "be able to run explainQuery" in {
        val filter = ECQL.toFilter("INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))")
        val query = new Query(defaultSft.getTypeName, filter)

        val out = new ExplainString()
        ds.getQueryPlan(query, explainer = out)

        val explanation = out.toString()
        explanation must not be null
        explanation.trim must not(beEmpty)
      }
    }

    "handle Query.ALL" in {
      ds.getFeatureSource(defaultSft.getTypeName).getFeatures(Query.ALL).features() must not throwAn[IllegalArgumentException]()
      ds.getFeatureReader(Query.ALL, Transaction.AUTO_COMMIT) must throwAn[IllegalArgumentException]
    }
  }
}
