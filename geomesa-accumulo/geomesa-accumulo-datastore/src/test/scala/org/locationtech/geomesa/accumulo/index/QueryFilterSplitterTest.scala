/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.AttributeExpression
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.tables.AvailableTables
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.filter
import org.locationtech.geomesa.utils.geotools.SftBuilder.Opts
import org.locationtech.geomesa.utils.geotools.{SftBuilder, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.filter._
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class QueryFilterSplitterTest extends Specification {

  val sft = new SftBuilder()
    .stringType("attr1")
    .stringType("attr2", index = true)
    .stringType("high", Opts(index = true, cardinality = Cardinality.HIGH))
    .stringType("low", Opts(index = true, cardinality = Cardinality.LOW))
    .date("dtg", default = true)
    .point("geom", default = true)
    .withIndexes(AvailableTables.DefaultTablesStr)
    .build("QueryFilterSplitterTest")

  val ff = CommonFactoryFinder.getFilterFactory2
  val splitter = new QueryFilterSplitter(sft)

  val geom                = "BBOX(geom,40,40,50,50)"
  val geom2               = "BBOX(geom,60,60,70,70)"
  val geomOverlap         = "BBOX(geom,35,35,55,55)"
  val dtg                 = "dtg DURING 2014-01-01T00:00:00Z/2014-01-01T23:59:59Z"
  val dtg2                = "dtg DURING 2014-01-02T00:00:00Z/2014-01-02T23:59:59"
  val dtgOverlap          = "dtg DURING 2014-01-01T00:00:00Z/2014-01-02T23:59:59Z"
  val nonIndexedAttr      = "attr1 = 'test'"
  val nonIndexedAttr2     = "attr1 = 'test2'"
  val indexedAttr         = "attr2 = 'test'"
  val indexedAttr2        = "attr2 = 'test2'"
  val highCardinaltiyAttr = "high = 'test'"
  val lowCardinaltiyAttr  = "low = 'test'"

  val wholeWorld          = "BBOX(geom,-180,-90,180,90)"

  def and(clauses: String*) = ff.and(clauses.map(ECQL.toFilter))
  def or(clauses: String*)  = ff.or(clauses.map(ECQL.toFilter))
  def not(clauses: String*)  = filter.andFilters(clauses.map(ECQL.toFilter).map(ff.not))(ff)
  def f(filter: String)     = ECQL.toFilter(filter)

  implicit def filterToString(f: Filter): String = ECQL.toCQL(f)
  implicit def stringToFilter(f: String): Filter = ECQL.toFilter(f)

  "QueryFilterSplitter" should {
    "return for filter include" >> {
      val filter = Filter.INCLUDE
      val options = splitter.getQueryOptions(Filter.INCLUDE)
      options must haveLength(1)
      options.head.filters must haveLength(1)
      options.head.filters.head.strategy mustEqual StrategyType.RECORD
      options.head.filters.head.primary mustEqual Seq(filter)
      options.head.filters.head.secondary must beNone
    }
    "return none for filter exclude" >> {
      val options = splitter.getQueryOptions(Filter.EXCLUDE)
      options must beEmpty
    }
    "return none for exclusive anded geoms" >> {
      val options = splitter.getQueryOptions(and(geom, geom2, dtg))
      options must beEmpty
    }.pendingUntilFixed("not implemented")
    "return none for exclusive anded dates" >> {
      val options = splitter.getQueryOptions(and(geom, dtg, dtg2))
      options must beEmpty
    }.pendingUntilFixed("not implemented")
    "work for spatio-temporal queries" >> {
      "with a simple and" >> {
        val filter = and(geom, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(f(geom), f(dtg))
        options.head.filters.head.secondary must beNone
      }
      "with multiple geometries" >> {
        val filter = and(geom, geom2, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(f(geom), f(geom2), f(dtg))
        options.head.filters.head.secondary must beNone
      }
      "with multiple dates" >> {
        val filter = and(geom, dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(f(geom), f(dtg), f(dtgOverlap))
        options.head.filters.head.secondary must beNone
      }
      "with multiple geometries and dates" >> {
        val filter = and(geom, geomOverlap, dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary must containTheSameElementsAs(Seq(f(geom), f(geomOverlap), f(dtg), f(dtgOverlap)))
        options.head.filters.head.secondary must beNone
      }
      "with simple ors" >> {
        val filter = or(and(geom, dtg), and(geom2, dtg2))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(2)
        forall(options.head.filters)(_.strategy mustEqual StrategyType.Z3)
        options.head.filters.map(_.primary) must
            containTheSameElementsAs(Seq(Seq(f(geom), f(dtg)), Seq(f(geom2), f(dtg2))))
        options.head.filters.map(_.secondary).filter(_.isDefined) must haveLength(1)
        options.head.filters.map(_.secondary).filter(_.isDefined).head.get must beAnInstanceOf[Not]
      }
      "while ignoring world-covering geoms" >> {
        val filter = f(wholeWorld)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.RECORD
        options.head.filters.head.primary mustEqual Seq(Filter.INCLUDE)
        options.head.filters.head.secondary must beNone
      }
      "while ignoring world-covering geoms when other filters are present" >> {
        val filter = and(wholeWorld, geom, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(f(geom), f(dtg))
        options.head.filters.head.secondary must beNone
      }
    }
    "work for single clause filters" >> {
      "spatial" >> {
        val filter = f(geom)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ST
        options.head.filters.head.primary mustEqual Seq(filter)
        options.head.filters.head.secondary must beNone
      }
      "temporal" >> {
        val filter = f(dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(dtg).map(f)
        options.head.filters.head.secondary must beNone
      }
      "non-indexed attributes" >> {
        val filter = f(nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.RECORD
        options.head.filters.head.primary mustEqual Seq(Filter.INCLUDE)
        options.head.filters.head.secondary must beSome(filter)
      }
      "indexed attributes" >> {
        val filter = f(indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary mustEqual Seq(filter)
        options.head.filters.head.secondary must beNone
      }
      "low-cardinality attributes" >> {
        val filter = f(lowCardinaltiyAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary mustEqual Seq(filter)
        options.head.filters.head.secondary must beNone
      }
      "high-cardinality attributes" >> {
        val filter = f(highCardinaltiyAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary mustEqual Seq(filter)
        options.head.filters.head.secondary must beNone
      }
    }
    "work for simple ands" >> {
      "spatial" >> {
        val filter = and(geom, geom2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ST
        options.head.filters.head.primary mustEqual Seq(geom, geom2).map(f)
        options.head.filters.head.secondary must beNone
      }
      "temporal" >> {
        val filter = and(dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(dtg, dtgOverlap).map(f)
        options.head.filters.head.secondary must beNone
      }
      "non-indexed attributes" >> {
        val filter = and(nonIndexedAttr, nonIndexedAttr2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.RECORD
        options.head.filters.head.primary mustEqual Seq(Filter.INCLUDE)
        options.head.filters.head.secondary must beSome(filter)
      }
      "indexed attributes" >> {
        val filter = and(indexedAttr, indexedAttr2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary.head mustEqual f(indexedAttr)
        options.head.filters.head.secondary.head mustEqual f(indexedAttr2)
      }
      "low-cardinality attributes" >> {
        val filter = and(lowCardinaltiyAttr, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary mustEqual Seq(f(lowCardinaltiyAttr))
        options.head.filters.head.secondary must beSome(f(nonIndexedAttr))
      }
    }
    "split filters on AND" >> {
      "with spatiotemporal clauses" >> {
        val filter = and(geom, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(geom, dtg).map(f)
        options.head.filters.head.secondary must beNone
      }
      "filters with spatiotemporal and non-indexed attributes clauses" >> {
        val filter = and(geom, dtg, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(geom, dtg).map(f)
        options.head.filters.head.secondary must beSome(f(nonIndexedAttr))
      }
      "with spatiotemporal and indexed attributes clauses" >> {
        val filter = and(geom, dtg, indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3)
        z3 must beSome
        z3.get.filters.head.primary mustEqual Seq(geom, dtg).map(f)
        z3.get.filters.head.secondary must beSome(f(indexedAttr))
        val attr = options.find(_.filters.head.strategy == StrategyType.ATTRIBUTE)
        attr must beSome
        attr.get.filters.head.primary mustEqual Seq(f(indexedAttr))
        attr.get.filters.head.secondary must beSome(and(geom, dtg))
      }
      "with spatiotemporal, indexed and non-indexed attributes clauses" >> {
        val filter = and(geom, dtg, indexedAttr, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3)
        z3 must beSome
        z3.get.filters.head.primary mustEqual Seq(geom, dtg).map(f)
        z3.get.filters.head.secondary must beSome(and(indexedAttr, nonIndexedAttr))
        val attr = options.find(_.filters.head.strategy == StrategyType.ATTRIBUTE)
        attr must beSome
        attr.get.filters.head.primary mustEqual Seq(f(indexedAttr))
        attr.get.filters.head.secondary must beSome(and(geom, dtg, nonIndexedAttr))
      }
    }
    "split filters on OR" >> {
      "with spatiotemporal clauses" >> {
        val filter = or(geom, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(2)
        val z3 = options.head.filters.find(_.strategy == StrategyType.Z3)
        z3 must beSome
        z3.get.primary mustEqual Seq(dtg).map(f)
        z3.get.secondary must beSome(not(geom))
        val st = options.head.filters.find(_.strategy == StrategyType.ST)
        st must beSome
        st.get.primary mustEqual Seq(f(geom))
        st.get.secondary must beNone
      }
      "with multiple spatial clauses" >> {
        val filter = or(geom, geom2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(2)
        forall(options.head.filters)(_.strategy mustEqual StrategyType.ST)
        options.head.filters.head.primary mustEqual Seq(f(geom))
        options.head.filters.head.secondary must beNone
        options.head.filters.tail.head.primary mustEqual Seq(f(geom2))
        options.head.filters.tail.head.secondary must beSome(not(geom))
      }
      "with spatiotemporal and indexed attribute clauses" >> {
        val filter = or(geom, indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(2)
        options.head.filters.map(_.strategy) must containTheSameElementsAs(Seq(StrategyType.ST, StrategyType.ATTRIBUTE))
        options.head.filters.find(_.strategy == StrategyType.ST).get.primary mustEqual Seq(f(geom))
        options.head.filters.find(_.strategy == StrategyType.ST).get.secondary must beNone
        options.head.filters.find(_.strategy == StrategyType.ATTRIBUTE).get.primary mustEqual Seq(f(indexedAttr))
        options.head.filters.find(_.strategy == StrategyType.ATTRIBUTE).get.secondary must beSome(not(geom))
      }
      "and collapse overlapping query filters" >> {
        "with spatiotemporal and non-indexed attribute clauses" >> {
          val filter = or(geom, nonIndexedAttr)
          val options = splitter.getQueryOptions(filter)
          options must haveLength(1)
          options.head.filters must haveLength(1)
          options.head.filters.head.strategy mustEqual StrategyType.RECORD
          options.head.filters.head.primary mustEqual Seq(Filter.INCLUDE)
          options.head.filters.head.secondary must beSome(filter)
        }
        "with overlapping geometries" >> {
          val filter = or(geom, geomOverlap)
          val options = splitter.getQueryOptions(filter)
          options must haveLength(1)
          options.head.filters must haveLength(1)
          options.head.filters.head.strategy mustEqual StrategyType.ST
          options.head.filters.head.primary mustEqual Seq(f(geomOverlap))
          options.head.filters.head.secondary must beNone
        }.pendingUntilFixed("not implemented")
        "with overlapping dates" >> {
          val filter = or(dtg, dtgOverlap)
          val options = splitter.getQueryOptions(filter)
          options must haveLength(1)
          options.head.filters must haveLength(1)
          options.head.filters.head.strategy mustEqual StrategyType.Z3
          options.head.filters.head.primary mustEqual Seq(wholeWorld, dtgOverlap).map(f)
          options.head.filters.head.secondary must beNone
        }.pendingUntilFixed("not implemented")
        "with overlapping geometries and dates" >> {
          val filter = or(and(geom, dtg), and(geomOverlap, dtgOverlap))
          val options = splitter.getQueryOptions(filter)
          options must haveLength(1)
          options.head.filters must haveLength(1)
          options.head.filters.head.strategy mustEqual StrategyType.Z3
          options.head.filters.head.primary mustEqual Seq(geomOverlap, dtgOverlap).map(f)
          options.head.filters.head.secondary must beNone
        }.pendingUntilFixed("not implemented")
      }
    }
    "split nested filters" >> {
      "with ANDs" >> {
        val filter = and(geom, and(dtg, nonIndexedAttr))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary mustEqual Seq(geom, dtg).map(f)
        options.head.filters.head.secondary must beSome(f(nonIndexedAttr))
      }
      "with ORs" >> {
        val filter = or(geom, or(dtg, indexedAttr))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(3)
        options.head.filters.map(_.strategy) must
            containTheSameElementsAs(Seq(StrategyType.Z3, StrategyType.ST, StrategyType.ATTRIBUTE))
        options.head.filters.map(_.primary) must
            containTheSameElementsAs(Seq(Seq(f(geom)), Seq(f(dtg)), Seq(f(indexedAttr))))
        options.head.filters.map(_.secondary) must
            containTheSameElementsAs(Seq(None, Some(not(geom)), Some(not(geom, dtg))))
      }
      "with ANDs and ORs" >> {
        "with spatiotemporal clauses and non-indexed attributes" >> {
          val filter = f(and(geom, dtg, or(nonIndexedAttr, nonIndexedAttr2)))
          val options = splitter.getQueryOptions(filter)
          options must haveLength(1)
          options.head.filters must haveLength(1)
          options.head.filters.head.strategy mustEqual StrategyType.Z3
          options.head.filters.head.primary mustEqual Seq(geom, dtg).map(f)
          options.head.filters.head.secondary must beSome(or(nonIndexedAttr, nonIndexedAttr2))
        }
      }
    }
    "support indexed date attributes" >> {
      val sft = SimpleFeatureTypes.createType("dtgIndex", "dtg:Date:index=full,*geom:Point:srid=4326")
      val splitter = new QueryFilterSplitter(sft)
      val filter = f("dtg TEQUALS 2014-01-01T12:30:00.000Z")
      val options = splitter.getQueryOptions(filter)
      options must haveLength(2)
      val z3 = options.find(_.filters.exists(_.strategy == StrategyType.Z3))
      z3 must beSome
      z3.get.filters must haveLength(1)
      z3.get.filters.head.primary mustEqual Seq(filter)
      z3.get.filters.head.secondary must beNone
      val attr = options.find(_.filters.exists(_.strategy == StrategyType.ATTRIBUTE))
      attr must beSome
      attr.get.filters must haveLength(1)
      attr.get.filters.head.primary mustEqual Seq(filter)
      attr.get.filters.head.secondary must beNone
    }
    "provide only one option on OR queries of high cardinality indexed attributes" >> {
      def testHighCard(attrPart: String): MatchResult[Any] = {
        val filterString = s"($attrPart) AND BBOX(geom, 40.0,40.0,50.0,50.0) AND dtg DURING 2014-01-01T00:00:00+00:00/2014-01-01T23:59:59+00:00"
        val options = splitter.getQueryOptions(f(filterString))
        options must haveLength(2)

        val attrOpt = options.find(_.filters.exists(_.strategy == StrategyType.ATTRIBUTE)).get
        attrOpt.filters.length mustEqual 1
        val attrQueryFilter = attrOpt.filters.head
        attrQueryFilter.strategy mustEqual StrategyType.ATTRIBUTE
        attrQueryFilter.primary.length mustEqual 5
        attrQueryFilter.primary.forall(_ must beAnInstanceOf[PropertyIsEqualTo])
        val attrProps = attrQueryFilter.primary.map(_.asInstanceOf[PropertyIsEqualTo])
        foreach(attrProps) {_.getExpression1.asInstanceOf[AttributeExpression].getPropertyName mustEqual "high" }
        attrQueryFilter.secondary.isDefined mustEqual true
        attrQueryFilter.secondary.get must beAnInstanceOf[And]
        attrQueryFilter.secondary.get.asInstanceOf[And].getChildren.length mustEqual 2

        val z3Opt = options.find(_.filters.exists(_.strategy == StrategyType.Z3)).get
        z3Opt.filters.length mustEqual 1
        val z3QueryFilters = z3Opt.filters.head
        z3QueryFilters.strategy mustEqual StrategyType.Z3
        z3QueryFilters.primary.length mustEqual 2
        z3QueryFilters.secondary.get must beAnInstanceOf[Or]
        val z3Props = z3QueryFilters.secondary.get.asInstanceOf[Or].getChildren.map(_.asInstanceOf[PropertyIsEqualTo])
        foreach (z3Props) { _.getExpression1.asInstanceOf[AttributeExpression].getPropertyName mustEqual "high" }
      }

      val orQuery = (0 until 5).map( i => s"high = 'h$i'").mkString(" OR ")
      val inQuery = s"high in (${(0 until 5).map( i => s"'h$i'").mkString(",")})"
      Seq(orQuery, inQuery).forall(testHighCard)
    }
  }
}
