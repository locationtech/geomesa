/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.NamedIndex
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.opengis.filter.{And, Filter}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

//Expand the test - https://geomesa.atlassian.net/browse/GEOMESA-308
@RunWith(classOf[JUnitRunner])
class QueryStrategyDeciderTest extends Specification with TestWithDataStore {

  import org.locationtech.geomesa.filter.ff

  override val spec = "nameHighCardinality:String:index=join:cardinality=high,ageJoinIndex:Long:index=join," +
      "heightFullIndex:Float:index=full,dtgJoinIndex:Date:index=join,weightNoIndex:String," +
      "dtgNoIndex:Date,dtg:Date,*geom:Point:srid=4326"

  addFeatures {
    val r = new Random(-57L)
    (0 until 1000).map { i =>
      val id = f"$i%03d"
      val name = s"name$id"
      val age = 20 + i % 40
      val height = 5.5 + (i % 10) / 10.0
      val weight = 150 + (1000 - i) % 70
      val noDtg = f"2016-02-${(i % 27) + 1}%02dT${i % 24}%02d:00:00.000Z"
      val dtg = f"2016-03-${(i % 30) + 1}%02dT${i % 24}%02d:00:00.000Z"
      val geom = {
        val tens = (i % 10) * -10
        val ones = r.nextDouble() * (if (r.nextBoolean()) 5 else -5)
        val dec = i % 100
        s"POINT (${tens + ones.toInt}.$dec 50.0)"
      }
      val sf = new ScalaSimpleFeature(sft, id)
      sf.setAttributes(Array(name, age, height, dtg, weight, noDtg, dtg, geom).asInstanceOf[Array[AnyRef]])
      sf
    }
  }

  // run stats so we have the latest
  ds.stats.generateStats(sft)

  "Cost-based strategy decisions" should {

    def getStrategies(filter: Filter, transforms: Option[Array[String]], explain: Explainer): Seq[FilterStrategy] = {
      val query = transforms.map(new Query(sftName, filter, _)).getOrElse(new Query(sftName, filter))
      query.getHints.put(QueryHints.COST_EVALUATION, CostEvaluation.Stats)
      ds.getQueryPlan(query, explainer = explain).map(_.filter)
    }

    def getStrategy(filter: String, expected: NamedIndex, transforms: Option[Array[String]], explain: Explainer) = {
      val strategies = getStrategies(ECQL.toFilter(filter), transforms, explain)
      forall(strategies)(_.index.name mustEqual expected.name)
    }

    def getRecordStrategy(filter: String, transforms: Option[Array[String]] = None, explain: Explainer = ExplainNull) =
      getStrategy(filter, IdIndex, transforms, explain)
    def getAttributeStrategy(filter: String, transforms: Option[Array[String]] = None, explain: Explainer = ExplainNull) =
      getStrategy(filter, AttributeIndex, transforms, explain)
    def getAttributeJoinStrategy(filter: String, transforms: Option[Array[String]] = None, explain: Explainer = ExplainNull) =
      getStrategy(filter, JoinIndex, transforms, explain)
    def getZ2Strategy(filter: String, transforms: Option[Array[String]] = None, explain: Explainer = ExplainNull) =
      getStrategy(filter, Z2Index, transforms, explain)
    def getZ3Strategy(filter: String, transforms: Option[Array[String]] = None, explain: Explainer = ExplainNull) =
      getStrategy(filter, Z3Index, transforms, explain)

    "select z3 over z2 when spatial is limiting factor" >> {
      getZ3Strategy("bbox(geom,-75,45,-70,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z")
    }

    "select strategy based on expected cost" >> {
      getZ2Strategy("bbox(geom,-120,49,-119,51) AND nameHighCardinality > 'name001'")
      getZ2Strategy("bbox(geom,-75,45,-65,55) AND ageJoinIndex > 30")
      getZ3Strategy("bbox(geom,-75,45,-65,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z AND " +
          "ageJoinIndex = 35")
      getAttributeJoinStrategy("bbox(geom,-75,45,-65,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z AND " +
          "ageJoinIndex = 35", Some(Array("geom", "dtg")))
      getAttributeJoinStrategy("bbox(geom,-75,45,-65,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z AND " +
          "ageJoinIndex = 35", Some(Array("geom", "dtg", "ageJoinIndex")))
      getAttributeJoinStrategy("bbox(geom,-75,45,-65,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z AND " +
          "nameHighCardinality > 'name990'")
      getAttributeJoinStrategy("bbox(geom,-75,45,-65,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z AND " +
          "nameHighCardinality IN ('name990', 'name991', 'name992', 'name993', 'name994')")
      getRecordStrategy("bbox(geom,-75,45,-65,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z AND " +
          "nameHighCardinality > 'name990' AND IN('name001', 'name002', 'name003')")
    }

    "select strategies that should result in zero rows scanned" >> {
      getZ2Strategy("bbox(geom,-75,0,-74.99,0.01) AND nameHighCardinality IN ('name990', 'name991', 'name992', 'name993', 'name994')")
      getAttributeJoinStrategy("bbox(geom,-75,45,-65,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z AND " +
          "nameHighCardinality > 'zzz'")
      getAttributeJoinStrategy("bbox(geom,-75,45,-65,55) AND dtg DURING 2016-03-01T00:00:00.000Z/2016-03-07T00:00:00.000Z AND " +
          "nameHighCardinality > 'zzz' AND IN('name001', 'name002', 'name003')")
    }
  }

  "Index-based strategy decisions" should {

    def getStrategies(filter: Filter, explain: Explainer = ExplainNull): Seq[FilterStrategy] = {
      // default behavior for this test is to use the index-based query costs
      val query = new Query(sftName, filter)
      query.getHints.put(QueryHints.COST_EVALUATION, CostEvaluation.Index)
      ds.getQueryPlan(query, explainer = explain).map(_.filter)
    }

    def getStrategy(filter: String, expected: NamedIndex, explain: Explainer = ExplainNull) = {
      val strategies = getStrategies(ECQL.toFilter(filter), explain)
      forall(strategies)(_.index.name mustEqual expected.name)
    }

    def getRecordStrategy(filter: String) = getStrategy(filter, IdIndex)
    def getAttributeStrategy(filter: String) = getStrategy(filter, AttributeIndex)
    def getAttributeJoinStrategy(filter: String) = getStrategy(filter, JoinIndex)
    def getZ2Strategy(filter: String) = getStrategy(filter, Z2Index)
    def getZ3Strategy(filter: String) = getStrategy(filter, Z3Index)
    def getFullTableStrategy(filter: String) = getZ3Strategy(filter)

    "Good spatial predicates should" >> {
      "get the z2 strategy" >> {
        forall(goodSpatialPredicates)(getZ2Strategy)
      }
    }

    "Indexed attribute filters should" >> {
      "get the attribute strategy for indexed attributes" >> {
        val predicates = Seq(
          "ageJoinIndex = '1000'",
          "ns:ageJoinIndex = '1000'",
          "nameHighCardinality LIKE '500%'",
          "ns:nameHighCardinality LIKE '500%'",
          "nameHighCardinality ILIKE '500%'",
          "ns:nameHighCardinality ILIKE '500%'",
          "ageJoinIndex <= 11",
          "ageJoinIndex < 11",
          "ageJoinIndex >= 11",
          "ageJoinIndex > 11",
          "11 > ageJoinIndex",
          "dtgJoinIndex DURING 2012-01-01T11:00:00.000Z/2014-01-01T12:15:00.000Z",
          "ns:dtgJoinIndex DURING 2012-01-01T11:00:00.000Z/2014-01-01T12:15:00.000Z",
          "dtgJoinIndex AFTER 2013-01-01T12:30:00.000Z",
          "dtgJoinIndex BEFORE 2014-01-01T12:30:00.000Z",
          "ns:dtgJoinIndex BEFORE 2014-01-01T12:30:00.000Z",
          "ageJoinIndex BETWEEN 10 and 20",
          "ageJoinIndex >= 11 AND ageJoinIndex < 20"
        )
        forall(predicates)(getAttributeJoinStrategy)
      }

      "get the attribute strategy for prefix filters on indexed attribute" >> {
        val predicates = Seq(
          "ageJoinIndex LIKE '500%'",
          "ns:ageJoinIndex LIKE '500%'",
          "ageJoinIndex ILIKE '500%'",
          "ns:ageJoinIndex ILIKE '500%'"
        )
        forall(predicates)(getAttributeStrategy)
      }.pendingUntilFixed("Lexicoders don't allow us to do prefix filters on non-strings")

      "find the best filter among several" >> {
        val ageFilter = ff.equals(ff.property("ageJoinIndex"), ff.literal(21))
        val nameFilter = ff.equals(ff.literal("foo"), ff.property("nameHighCardinality"))
        val heightFilter = ff.equals(ff.property("heightFullIndex"), ff.literal(12.0D))
        val weightFilter = ff.equals(ff.literal(21.12D), ff.property("weightNoIndex"))

        val primary = Some(FastFilterFactory.optimize(sft, nameFilter))
        val secondary = FastFilterFactory.optimize(sft, ff.and(Seq(heightFilter, weightFilter, ageFilter)))

        "when best is first" >> {
          val strats = getStrategies(ff.and(Seq(nameFilter, heightFilter, weightFilter, ageFilter)))
          strats must haveLength(1)
          strats.head.index.name mustEqual JoinIndex.name
          strats.head.primary mustEqual primary
          strats.head.secondary must beSome(secondary)
        }

        "when best is in the middle" >> {
          val strats = getStrategies(ff.and(Seq(ageFilter, nameFilter, heightFilter, weightFilter)))
          strats must haveLength(1)
          strats.head.index.name mustEqual JoinIndex.name
          strats.head.primary mustEqual primary
          strats.head.secondary must beSome(secondary)
        }

        "when best is last" >> {
          val strats = getStrategies(ff.and(Seq(ageFilter, heightFilter, weightFilter, nameFilter)))
          strats must haveLength(1)
          strats.head.index.name mustEqual JoinIndex.name
          strats.head.primary mustEqual primary
          strats.head.secondary must beSome(secondary)
        }

        "use best indexed attribute if like and retain all children for > 2 filters" >> {
          val like = ff.like(ff.property("nameHighCardinality"), "baddy")
          val strats = getStrategies(ff.and(Seq(like, heightFilter, weightFilter, ageFilter)))
          strats must haveLength(1)
          strats.head.index.name mustEqual AttributeIndex.name
          strats.head.primary must beSome(FastFilterFactory.optimize(sft, heightFilter))
          strats.head.secondary must beSome(FastFilterFactory.optimize(sft, ff.and(Seq(like, weightFilter, ageFilter))))
        }
      }
    }

    "Non-indexed attribute filters should" >> {
      "get full table strategy" >> {
        val predicates = Seq(
          "weightNoIndex = 'val56'",
          "weightNoIndex ILIKE '2nd1%'",
          "ns:weightNoIndex ILIKE '2nd1%'"
        )
        forall(predicates)(getFullTableStrategy)
      }
    }

    "Id filters should" >> {
      "get the records strategy for mixed id queries" >> {
        val predicates = Seq(
          "IN ('val56')",
          "IN('01','02')" ,
          "IN('03','05') AND IN('01')",
          "IN('01','02') AND ageJoinIndex = 100001",
          "IN('01','02') AND ageJoinIndex = 100001 AND IN('03','05')",
          "ageJoinIndex = '100001'  AND IN('01')" ,
          "IN ('val56') AND ageJoinIndex = 400",
          "IN('10')",
          "IN ('val56') AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
          "dtg DURING 2010-06-01T00:00:00.000Z/2010-08-31T23:59:59.000Z AND IN('01')",
          "IN('01') AND dtg DURING 2010-06-01T00:00:00.000Z/2010-08-31T23:59:59.000Z ",
          "WITHIN(geom, POLYGON ((40 20, 50 20, 50 30, 40 30, 40 20))) AND IN('01')",
          "IN('01') AND WITHIN(geom, POLYGON ((40 20, 50 20, 50 30, 40 30, 40 20)))",
          "dtg DURING 2010-06-01T00:00:00.000Z/2010-08-31T23:59:59.000Z AND IN('01','02')" +
              "AND WITHIN(geom, POLYGON ((40 20, 50 20, 50 30, 40 30, 40 20))) AND ageJoinIndex = '100001'",
          "INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))" +
              "AND IN('val56','val55') AND ageJoinIndex = 3000 AND IN('val59','val54') AND ageJoinIndex > '20'"
        )
        forall(predicates)(getRecordStrategy)
      }
    }

    "IS NOT NULL filters should" >> {
      "get the attribute strategy if attribute is indexed" >> {
        getAttributeJoinStrategy("ageJoinIndex IS NOT NULL")
      }
      "get full table strategy if attribute is not indexed" >> {
        getFullTableStrategy("weightNoIndex IS NOT NULL")
      }
    }

    "Spatio-temporal filters should" >> {

      val temporalPredicates = Seq(
        "dtg DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z",
        "ns:dtg DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z",
        "dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z'"
      )
      val spatioTemporalPredicates = goodSpatialPredicates.flatMap(s => temporalPredicates.map(t => s"$s AND $t"))

      val joinAttributePredicates = Seq(
        "ageJoinIndex = 100001",
        "ageJoinIndex ILIKE '1001%'"
      )

      "get the z3 strategy" >> {
        forall(spatioTemporalPredicates)(getZ3Strategy)
      }

      "prioritize z3 index over low-cardinality join indexed attributes" >> {
        val withAttributes = spatioTemporalPredicates.flatMap(st => joinAttributePredicates.map(a => s"$st AND $a"))
        forall(withAttributes)(getZ3Strategy)
      }

      "get the z3 strategy with only temporal filters" >> {
        forall(temporalPredicates)(getZ3Strategy)
      }

      "get the z3 strategy with whole world filters and temporal filters" >> {
        val withWholeWorld = temporalPredicates.map("BBOX(geom,-180,-90,180,90) AND " + _)
        forall(withWholeWorld)(getZ3Strategy)
      }

      "prioritize z3 index over low-cardinality join indexed attributes with only temporal filters" >> {
        val withAttributes = temporalPredicates.flatMap(st => joinAttributePredicates.map(a => s"$st AND $a"))
        forall(withAttributes)(getZ3Strategy)
      }

      "get the z2 strategy with non-bounded time intervals" >> {
        val predicates = Seq(
          "bbox(geom, 35, 59, 45, 70) AND dtg before 2010-05-12T12:00:00.000Z",
          "bbox(geom, 35, 59, 45, 70) AND dtg after 2010-05-12T12:00:00.000Z",
          "bbox(geom, 35, 59, 45, 70) AND dtg < '2010-05-12T12:00:00.000Z'",
          "bbox(geom, 35, 59, 45, 70) AND dtg <= '2010-05-12T12:00:00.000Z'",
          "bbox(geom, 35, 59, 45, 70) AND dtg > '2010-05-12T12:00:00.000Z'",
          "bbox(geom, 35, 59, 45, 70) AND dtg >= '2010-05-12T12:00:00.000Z'"
        )
        forall(predicates)(getZ2Strategy)
      }
    }

    "ANDed Attribute filters should" >> {
      "prioritize spatial filters over normal-cardinality join or non indexed attributes" >> {
        val predicates = Seq(
          "ageJoinIndex = 21 AND INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
          "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND ageJoinIndex = 21",
          "weightNoIndex = 'dummy' AND INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) " +
              "AND ageJoinIndex = 'dummy'",
          "weightNoIndex = 'dummy' AND INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
          "ageJoinIndex ILIKE '%1' AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
          "dtgNonIdx DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z AND " +
              "INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND ageJoinIndex = '100'",
          "ageJoinIndex = '100001' AND INTERSECTS(geom, POLYGON ((45 20, 48 20, 48 27, 45 27, 45 20)))",
          "ageJoinIndex = '100001' AND INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
          "ageJoinIndex ILIKE '2nd1%' AND CROSSES(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
          "ageJoinIndex ILIKE '2nd1%' AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
          "ageJoinIndex ILIKE '2nd1%' AND OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
          "ageJoinIndex ILIKE '2nd1%' AND WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
        )
        forall(predicates)(getZ2Strategy)
      }

      "prioritize spatial filters over normal-cardinality join or non indexed attributes with namespaces" >> {
        val predicates = Seq(
          "ns:ageJoinIndex = 21 AND INTERSECTS(ns:geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
          "INTERSECTS(ns:geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND ns:ageJoinIndex = 21",
          "ns:weightNoIndex = 'dummy' AND INTERSECTS(ns:geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND " +
              "ns:ageJoinIndex = 'dummy'",
          "ns:weightNoIndex = 'dummy' AND INTERSECTS(ns:geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
          "ns:ageJoinIndex ILIKE '%1' AND INTERSECTS(ns:geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
          "ns:dtgNonIdx DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z AND " +
              "INTERSECTS(ns:geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND ns:ageJoinIndex = '100'",
          "ns:ageJoinIndex = '100001' AND INTERSECTS(ns:geom, POLYGON ((45 20, 48 20, 48 27, 45 27, 45 20)))",
          "ns:ageJoinIndex = '100001' AND INTERSECTS(ns:geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
          "ns:ageJoinIndex ILIKE '2nd1%' AND CROSSES(ns:geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
          "ns:ageJoinIndex ILIKE '2nd1%' AND INTERSECTS(ns:geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
          "ns:ageJoinIndex ILIKE '2nd1%' AND OVERLAPS(ns:geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
          "ns:ageJoinIndex ILIKE '2nd1%' AND WITHIN(ns:geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
        )
        forall(predicates)(getZ2Strategy)
      }

      "get the attribute strategy when other predicates are not indexed" >> {
        val predicates = Seq(
          "ageJoinIndex = '100001' AND DISJOINT(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))", // disjoint is not covered by stidx
          "ageJoinIndex = '100'",
          "weightNoIndex = 'val56' AND ageJoinIndex = '100'",
          "ageJoinIndex = '100' AND weightNoIndex = 'val3'",
          "weightNoIndex = 'val56' AND weightNoIndex = 'val57' AND ageJoinIndex = '100'",
          "nameHighCardinality = 'val56' AND dtg DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z",
          "dtg DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z AND nameHighCardinality = 'val56'",
          "ageJoinIndex = '100' AND NOT (INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))"
        )
        forall(predicates)(getAttributeJoinStrategy)
      }

      "respect high cardinality attributes regardless of order" >> {
        val attr = "nameHighCardinality = 'test'"
        val geom = "BBOX(geom, -10,-10,10,10)"
        getAttributeJoinStrategy(s"$attr AND $geom")
        getAttributeJoinStrategy(s"$geom AND $attr")
      }

      "respect cardinality with multiple attributes" >> {
        val attrNoIndex = "weightNoIndex = 'test'"
        val attrIndex = "nameHighCardinality = 'test'"
        val geom = "BBOX(geom, -10,-10,10,10)"
        getAttributeJoinStrategy(s"$geom AND $attrNoIndex AND $attrIndex")
        getAttributeJoinStrategy(s"$geom AND $attrIndex AND $attrNoIndex")
        getAttributeJoinStrategy(s"$attrNoIndex AND $attrIndex AND $geom")
        getAttributeJoinStrategy(s"$attrIndex AND $attrNoIndex AND $geom")
        getAttributeJoinStrategy(s"$attrNoIndex AND $geom AND $attrIndex")
        getAttributeJoinStrategy(s"$attrIndex AND $geom AND $attrNoIndex")
      }
    }

    "Single Attribute, indexed, high cardinality OR queries should" >> {
      "select an single attribute index scan with multiple ranges" >> {

        import org.locationtech.geomesa.filter.decomposeOr

        val st = " AND BBOX(geom, 40.0,40.0,50.0,50.0) AND dtg DURING 2014-01-01T00:00:00+00:00/2014-01-01T23:59:59+00:00"
        val orQuery = (0 until 5).map(i => s"nameHighCardinality = 'h$i'").mkString("(", " OR ", ")")
        val inQuery = "(nameHighCardinality IN ('a','b','c','d','e'))"

        forall(Seq(orQuery, inQuery)) { filter =>
          val strats = getStrategies(ECQL.toFilter(s"$filter $st"))
          strats must haveLength(1)
          strats.head.index.name mustEqual JoinIndex.name
          strats.head.primary must beSome
          decomposeOr(strats.head.primary.get) must
              containTheSameElementsAs(decomposeOr(FastFilterFactory.toFilter(sft, filter)))
          strats.head.secondary must beSome
          strats.head.secondary.get must beAnInstanceOf[And]
          strats.head.secondary.get.asInstanceOf[And].getChildren must haveLength(2)
        }
      }
    }
  }
}
