/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.CURRENT_SCHEMA_VERSION
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.filter.visitor.LocalNameVisitorImpl
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SftBuilder.Opts
import org.locationtech.geomesa.utils.geotools.{SftBuilder, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.filter.{And, Filter}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

//Expand the test - https://geomesa.atlassian.net/browse/GEOMESA-308
@RunWith(classOf[JUnitRunner])
class QueryStrategyDeciderTest extends Specification {

  val sftIndex = new SftBuilder()
    .intType("id")
    .point("geom", default = true)
    .date("dtg", default = true)
    .stringType("attr1")
    .stringType("attr2", index = true)
    .stringType("high", Opts(index = true, cardinality = Cardinality.HIGH))
    .stringType("low", Opts(index = true, cardinality = Cardinality.LOW))
    .date("dtgNonIdx")
    .build("feature")

  val sftNonIndex = new SftBuilder()
    .intType("id")
    .point("geom", default = true)
    .date("dtg", default = true)
    .stringType("attr1")
    .stringType("attr2")
    .build("featureNonIndex")

  def getStrategy(filterString: String, version: Int = CURRENT_SCHEMA_VERSION): Strategy = {
    val sft = if (version > 0) sftIndex else sftNonIndex
    sft.setSchemaVersion(version)
    val filter = ECQL.toFilter(filterString)
    val hints = new UserDataStrategyHints()
    val query = new Query(sft.getTypeName)
    query.setFilter(filter.accept(new LocalNameVisitorImpl(sft), null).asInstanceOf[Filter])
    val strats = QueryStrategyDecider.chooseStrategies(sft, query, hints, None)
    strats must haveLength(1)
    strats.head
  }

  def getStrategyT[T <: Strategy](filterString: String, ct: ClassTag[T]) =
    getStrategy(filterString) must beAnInstanceOf[T](ct)

  def getRecordStrategy(filterString: String) =
    getStrategyT(filterString, ClassTag(classOf[RecordIdxStrategy]))
  def getStStrategy(filterString: String) =
    getStrategyT(filterString, ClassTag(classOf[STIdxStrategy]))
  def getAttributeIdxStrategy(filterString: String) =
    getStrategyT(filterString, ClassTag(classOf[AttributeIdxStrategy]))
  def getZ3Strategy(filterString: String) =
    getStrategyT(filterString, ClassTag(classOf[Z3IdxStrategy]))

  "Good spatial predicates" should {
    "get the stidx strategy" in {
      forall(goodSpatialPredicates){ getStStrategy }
    }
  }

  "Attribute filters" should {
    "get the attribute equals strategy" in {
      getAttributeIdxStrategy("attr2 = 'val56'")
    }

    "get the attribute equals strategy for namespaced attribute" in {
      getAttributeIdxStrategy("ns:attr2 = 'val56'")
    }

    "get the record strategy for non indexed attributes" in {
      getRecordStrategy("attr1 = 'val56'")
    }

    "get the attribute likes strategy" in {
      val fs = "attr2 ILIKE '2nd1%'"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute likes strategy for namespaced attribute" in {
      val fs = "ns:attr2 ILIKE '2nd1%'"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the record strategy if attribute non-indexed" in {
      getRecordStrategy("attr1 ILIKE '2nd1%'")
    }

    "get the record strategy if attribute non-indexed for a namespaced attribute" in {
      getRecordStrategy("ns:attr1 ILIKE '2nd1%'")
    }

    "get the attribute strategy for lte" in {
      val fs = "attr2 <= 11"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for lt" in {
      val fs = "attr2 < 11"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for gte" in {
      val fs = "attr2 >= 11"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for gt" in {
      val fs = "attr2 > 11"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for gt prop on right" in {
      val fs = "11 > attr2"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for during" in {
      val fs = "attr2 DURING 2012-01-01T11:00:00.000Z/2014-01-01T12:15:00.000Z"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for during for a namespaced attributes" in {
      val fs = "ns:attr2 DURING 2012-01-01T11:00:00.000Z/2014-01-01T12:15:00.000Z"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for after" in {
      val fs = "attr2 AFTER 2013-01-01T12:30:00.000Z"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for before" in {
      val fs = "attr2 BEFORE 2014-01-01T12:30:00.000Z"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for before for a namespaced attributes" in {
      val fs = "ns:attr2 BEFORE 2014-01-01T12:30:00.000Z"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for between" in {
      val fs = "attr2 BETWEEN 10 and 20"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "get the attribute strategy for ANDed attributes" in {
      val fs = "attr2 >= 11 AND attr2 < 20"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }

    "partition a query by selecting the best filter" >> {
      val sftName = "attributeQuerySplitTest"
      val spec = "name:String:index=true:cardinality=high," +
          "age:Integer:index=true:cardinality=low," +
          "weight:Double:index=false," +
          "height:Float:index=false:cardinality=unknown," +
          "count:Integer:index=true:cardinality=low," +
          "*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType(sftName, spec)

      val ff = CommonFactoryFinder.getFilterFactory(null)
      val ageFilter = ff.equals(ff.property("age"), ff.literal(21))
      val nameFilter = ff.equals(ff.literal("foo"), ff.property("name"))
      val heightFilter = ff.equals(ff.property("height"), ff.literal(12.0D))
      val weightFilter = ff.equals(ff.literal(21.12D), ff.property("weight"))

      val hints = new UserDataStrategyHints()

      "when best is first" >> {
        val filter = ff.and(Seq(nameFilter, heightFilter, weightFilter, ageFilter))
        val primary = Seq(nameFilter)
        val secondary = ff.and(Seq(heightFilter, weightFilter, ageFilter))

        val query = new Query(sft.getTypeName, filter)
        val strats = QueryStrategyDecider.chooseStrategies(sft, query, hints, None)

        strats must haveLength(1)
        strats.head.filter.strategy mustEqual StrategyType.ATTRIBUTE
        strats.head.filter.primary mustEqual primary
        strats.head.filter.secondary must beSome(secondary)
      }

      "when best is in the middle" >> {
        val filter = ff.and(Seq[Filter](ageFilter, nameFilter, heightFilter, weightFilter))
        val primary = Seq(nameFilter)
        val secondary = ff.and(Seq(heightFilter, weightFilter, ageFilter))

        val query = new Query(sft.getTypeName, filter)
        val strats = QueryStrategyDecider.chooseStrategies(sft, query, hints, None)

        strats must haveLength(1)
        strats.head.filter.strategy mustEqual StrategyType.ATTRIBUTE
        strats.head.filter.primary mustEqual primary
        strats.head.filter.secondary must beSome(secondary)
      }

      "when best is last" >> {
        val filter = ff.and(Seq[Filter](ageFilter, heightFilter, weightFilter, nameFilter))
        val primary = Seq(nameFilter)
        val secondary = ff.and(Seq(heightFilter, weightFilter, ageFilter))

        val query = new Query(sft.getTypeName, filter)
        val strats = QueryStrategyDecider.chooseStrategies(sft, query, hints, None)

        strats must haveLength(1)
        strats.head.filter.strategy mustEqual StrategyType.ATTRIBUTE
        strats.head.filter.primary mustEqual primary
        strats.head.filter.secondary must beSome(secondary)
      }

      "use best indexable attribute if like and retain all children for > 2 filters" in {
        val filter = ECQL.toFilter("name LIKE 'baddy' AND age=21 AND count<5")
        val query = new Query(sft.getTypeName, filter)

        val strats = QueryStrategyDecider.chooseStrategies(sft, query, hints, None)

        strats must haveLength(1)
        strats.head.filter.strategy mustEqual StrategyType.ATTRIBUTE
        strats.head.filter.primary mustEqual Seq(ECQL.toFilter("name LIKE 'baddy'"))
        strats.head.filter.secondary must beSome(ECQL.toFilter("age=21 AND count<5"))
      }
    }
  }

  "Attribute filters" should {
    "get the record strategy if not catalog" in {
      getRecordStrategy("attr1 ILIKE '2nd1%'")
    }
  }

  "Id filters" should {
    "get the attribute equals strategy" in {
      val fs = "IN ('val56')"
      getStrategy(fs) must beAnInstanceOf[RecordIdxStrategy]
    }
  }

  "Id and Spatio-temporal filters" should {
    "get the records strategy" in {
      val fs = "IN ('val56') AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
      getStrategy(fs) must beAnInstanceOf[RecordIdxStrategy]
    }
  }

  "Id and Attribute filters" should {
    "get the records strategy" in {
      val fs = "IN ('val56') AND attr2 = val56"
      getStrategy(fs) must beAnInstanceOf[RecordIdxStrategy]
    }
  }

  "Really complicated Id AND * filters" should {
    "get the records strategy" in {
      val fsFragment1="INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
      val fsFragment2="AND IN ('val56','val55') AND attr2 = val56 AND IN('val59','val54') AND attr2 = val60"
      val fs = s"$fsFragment1 $fsFragment2"
      getStrategy(fs) must beAnInstanceOf[RecordIdxStrategy]
    }
  }

  "IS NOT NULL filters" should {
    "get the attribute strategy if attribute is indexed" in {
      val fs = "attr2 IS NOT NULL"
      getStrategy(fs) must beAnInstanceOf[AttributeIdxStrategy]
    }
    "get the stidx strategy if attribute is not indexed" in {
      getRecordStrategy("attr1 IS NOT NULL")
    }
  }

  "Anded Attribute filters" should {
    "get the STIdx strategy with stIdxStrategyPredicates" in {
      forall(stIdxStrategyPredicates) { getStStrategy }
    }

    "get the STIdx strategy with stIdxStrategyPredicates with namespaces" in {
      forall(stIdxStrategyPredicatesWithNS) { getStStrategy }
    }

    "get the stidx strategy with attributeAndGeometricPredicates" in {
      forall(attributeAndGeometricPredicates) { getStStrategy }
    }

    "get the stidx strategy with attributeAndGeometricPredicates with namespaces" in {
      forall(attributeAndGeometricPredicatesWithNS) { getStStrategy }
    }

    "get the record strategy for non-indexed queries" in {
      forall(idPredicates ++ nonIndexedPredicates) { getRecordStrategy }
    }

    "get the z3 strategy with spatio-temporal queries" in {
      forall(spatioTemporalPredicates) { getZ3Strategy }
      val morePredicates = temporalPredicates.drop(1).flatMap(p => goodSpatialPredicates.map(_ + " AND " + p))
      forall(morePredicates) { getZ3Strategy }
      val withAttrs = temporalPredicates.drop(1).flatMap(p => attributeAndGeometricPredicates.map(_ + " AND " + p))
      forall(withAttrs) { getZ3Strategy }
      val wholeWorld = "BBOX(geom,-180,-90,180,90) AND dtg DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z"
      getZ3Strategy(wholeWorld)
    }

    "get the z3 strategy with temporal queries" in {
      forall(z3Predicates) { getZ3Strategy }
    }

    "get the stidx strategy with non-bounded time intervals" in {
      val predicates = Seq(
        "bbox(geom, 35, 59, 45, 70) AND dtg before 2010-05-12T12:00:00.000Z",
        "bbox(geom, 35, 59, 45, 70) AND dtg after 2010-05-12T12:00:00.000Z",
        "bbox(geom, 35, 59, 45, 70) AND dtg < '2010-05-12T12:00:00.000Z'",
        "bbox(geom, 35, 59, 45, 70) AND dtg <= '2010-05-12T12:00:00.000Z'",
        "bbox(geom, 35, 59, 45, 70) AND dtg > '2010-05-12T12:00:00.000Z'",
        "bbox(geom, 35, 59, 45, 70) AND dtg >= '2010-05-12T12:00:00.000Z'"
      )
      forall(predicates) { getStStrategy }
    }

    "get the attribute strategy with attrIdxStrategyPredicates" in {
      forall(attrIdxStrategyPredicates) { getAttributeIdxStrategy }
    }

    "respect high cardinality attributes regardless of order" in {
      val attr = "high = 'test'"
      val geom = "BBOX(geom, -10,-10,10,10)"
      getStrategy(s"$attr AND $geom") must beAnInstanceOf[AttributeIdxStrategy]
      getStrategy(s"$geom AND $attr") must beAnInstanceOf[AttributeIdxStrategy]
    }

    "respect low cardinality attributes regardless of order" in {
      val attr = "low = 'test'"
      val geom = "BBOX(geom, -10,-10,10,10)"
      getStrategy(s"$attr AND $geom") must beAnInstanceOf[STIdxStrategy]
      getStrategy(s"$geom AND $attr") must beAnInstanceOf[STIdxStrategy]
    }

    "respect cardinality with multiple attributes" in {
      val attr1 = "low = 'test'"
      val attr2 = "high = 'test'"
      val geom = "BBOX(geom, -10,-10,10,10)"
      getStrategy(s"$geom AND $attr1 AND $attr2") must beAnInstanceOf[AttributeIdxStrategy]
      getStrategy(s"$geom AND $attr2 AND $attr1") must beAnInstanceOf[AttributeIdxStrategy]
      getStrategy(s"$attr1 AND $attr2 AND $geom") must beAnInstanceOf[AttributeIdxStrategy]
      getStrategy(s"$attr2 AND $attr1 AND $geom") must beAnInstanceOf[AttributeIdxStrategy]
      getStrategy(s"$attr1 AND $geom AND $attr2") must beAnInstanceOf[AttributeIdxStrategy]
      getStrategy(s"$attr2 AND $geom AND $attr1") must beAnInstanceOf[AttributeIdxStrategy]
    }
  }

  "QueryStrategyDecider" should {
    "handle complex filters" in {
      skipped("debugging")
      implicit val ff = CommonFactoryFinder.getFilterFactory2
      val filter = ECQL.toFilter("BBOX(geom,-180,-90,180,90) AND " +
          "dtg DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z")
      println(filter)
      println(org.locationtech.geomesa.filter.rewriteFilterInDNF(filter))
      success
    }
  }

  "Single Attribute, indexed, high cardinality OR queries" should {
    "select an single attribute index scan with multiple ranges" in {

      "OR query" >> {
        val orQuery = (0 until 5).map( i => s"high = 'h$i'").mkString(" OR ")
        val fs = s"($orQuery) AND BBOX(geom, 40.0,40.0,50.0,50.0) AND dtg DURING 2014-01-01T00:00:00+00:00/2014-01-01T23:59:59+00:00"
        val strat = getStrategy(fs)
        strat must beAnInstanceOf[AttributeIdxStrategy]
        strat.filter.or mustEqual true
        strat.filter.strategy mustEqual StrategyType.ATTRIBUTE
        strat.filter.primary.length mustEqual 5
        strat.filter.secondary.isDefined mustEqual true
        strat.filter.secondary.get must beAnInstanceOf[And]
        strat.filter.secondary.get.asInstanceOf[And].getChildren.length mustEqual 2
      }

      "in query" >> {
        val fs = "(high IN ('a','b','c')) AND BBOX(geom, 40.0,40.0,50.0,50.0) AND dtg DURING 2014-01-01T00:00:00+00:00/2014-01-01T23:59:59+00:00"
        val strat = getStrategy(fs)
        strat must beAnInstanceOf[AttributeIdxStrategy]
      }
    }
  }
}
