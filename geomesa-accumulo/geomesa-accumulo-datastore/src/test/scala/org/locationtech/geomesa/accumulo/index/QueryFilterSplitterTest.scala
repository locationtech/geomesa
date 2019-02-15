/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.filter.{decomposeAnd, decomposeOr}
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndexFactory
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.planning.FilterSplitter
import org.locationtech.geomesa.utils.geotools.{SchemaBuilder, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.filter._
import org.opengis.filter.temporal.During
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class QueryFilterSplitterTest extends Specification {

  import org.locationtech.geomesa.filter.ff
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val sft = SchemaBuilder.builder()
    .addString("attr1")
    .addString("attr2").withIndex()
    .addString("attr3")
    .addString("attr4")
    .addString("high").withIndex(Cardinality.HIGH)
    .addString("low").withIndex(Cardinality.LOW)
    .addDate("dtg", default = true)
    .addPoint("geom", default = true)
    .build("QueryFilterSplitterTest")

  sft.setIndices(GeoMesaFeatureIndexFactory.indices(sft))

  val splitter = new FilterSplitter(sft, GeoMesaFeatureIndexFactory.create(null, sft, sft.getIndices), None)

  val geom                = "BBOX(geom,40,40,50,50)"
  val geom2               = "BBOX(geom,60,60,70,70)"
  val geomOverlap         = "BBOX(geom,35,35,55,55)"
  val dtg                 = "dtg DURING 2014-01-01T00:00:00Z/2014-01-01T23:59:59Z"
  val dtg2                = "dtg DURING 2014-01-02T00:00:00Z/2014-01-02T23:59:59Z"
  val dtgOverlap          = "dtg DURING 2014-01-01T00:00:00Z/2014-01-02T23:59:59Z"
  val nonIndexedAttr      = "attr1 = 'test'"
  val nonIndexedAttr2     = "attr1 = 'test2'"
  val indexedAttr         = "attr2 = 'test'"
  val indexedAttr2        = "attr2 = 'test2'"
  val highCardinaltiyAttr = "high = 'test'"
  val lowCardinaltiyAttr  = "low = 'test'"

  val wholeWorld          = "BBOX(geom,-180,-90,180,90)"

  val includeStrategy     = Z3Index.name

  def and(clauses: Filter*) = ff.and(clauses)
  def or(clauses: Filter*)  = ff.or(clauses)
  def and(clauses: String*)(implicit d: DummyImplicit) = ff.and(clauses.map(ECQL.toFilter))
  def or(clauses: String*)(implicit d: DummyImplicit)  = ff.or(clauses.map(ECQL.toFilter))
  def not(clauses: String*) = filter.andFilters(clauses.map(ECQL.toFilter).map(ff.not))(ff)
  def f(filter: String)     = ECQL.toFilter(filter)

  def compareAnd(primary: Option[Filter], clauses: Filter*): MatchResult[Option[Seq[Filter]]] =
    primary.map(decomposeAnd) must beSome(containTheSameElementsAs(clauses))
  def compareAnd(primary: Option[Filter], clauses: String*)
                (implicit d: DummyImplicit): MatchResult[Option[Seq[Filter]]] =
    compareAnd(primary, clauses.map(ECQL.toFilter): _*)

  def compareOr(primary: Option[Filter], clauses: Filter*): MatchResult[Option[Seq[Filter]]] =
    primary.map(decomposeOr) must beSome(containTheSameElementsAs(clauses))
  def compareOr(primary: Option[Filter], clauses: String*)
                (implicit d: DummyImplicit): MatchResult[Option[Seq[Filter]]] =
    compareOr(primary, clauses.map(ECQL.toFilter): _*)

  "QueryFilterSplitter" should {

    "return for filter include" >> {
      val filter = Filter.INCLUDE
      val options = splitter.getQueryOptions(Filter.INCLUDE)
      options must haveLength(1)
      options.head.strategies must haveLength(1)
      options.head.strategies.head.index.name mustEqual Z3Index.name
      options.head.strategies.head.primary must beNone
      options.head.strategies.head.secondary must beNone
    }

    "return none for filter exclude" >> {
      val options = splitter.getQueryOptions(Filter.EXCLUDE)
      options must beEmpty
    }

    "work for spatio-temporal queries" >> {
      "with a simple and" >> {
        val filter = and(geom, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containAllOf(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(f(geom))
        z2.strategies.head.secondary must beSome(f(dtg))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        z3.strategies.head.primary.map(decomposeAnd) must beSome(containTheSameElementsAs(Seq(f(geom), f(dtg))))
        z3.strategies.head.secondary must beNone
      }

      "with multiple geometries" >> {
        val filter = and(geom, geom2, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containAllOf(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        compareAnd(z2.strategies.head.primary, geom, geom2)
        z2.strategies.head.secondary must beSome(f(dtg))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, geom2, dtg)
        z3.strategies.head.secondary must beNone
      }

      "with multiple dates" >> {
        val filter = and(geom, dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containAllOf(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(f(geom))
        z2.strategies.head.secondary must beSome(and(dtg, dtgOverlap))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, dtg, dtgOverlap)
        z3.strategies.head.secondary must beNone
      }

      "with multiple geometries and dates" >> {
        val filter = and(geom, geomOverlap, dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containAllOf(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(and(geom, geomOverlap))
        z2.strategies.head.secondary must beSome(and(dtg, dtgOverlap))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, geomOverlap, dtg, dtgOverlap)
        z3.strategies.head.secondary must beNone
      }

      "with single attribute ors" >> {
        val filter = and(or(geom, geom2), f(dtg))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containTheSameElementsAs(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        compareOr(z2.strategies.head.primary, geom, geom2)
        forall(z2.strategies.map(_.secondary))(_ must beSome(beAnInstanceOf[During]))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, or(geom, geom2), f(dtg))
        z3.strategies.head.secondary must beNone
      }

      "with multiple attribute ors" >> {
        val filter = or(and(geom, dtg), and(geom2, dtg2))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containTheSameElementsAs(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        compareOr(z2.strategies.head.primary, geom, geom2)
        z2.strategies.head.secondary must beSome // secondary filter is too complex to compare here...
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        z3.strategies.head.primary must beSome
        z3.strategies.head.primary.get must beAnInstanceOf[And]
        z3.strategies.head.primary.get.asInstanceOf[And].getChildren must haveLength(2)
        z3.strategies.head.primary.get.asInstanceOf[And].getChildren.map(Option.apply) must
            contain(compareOr(_: Option[Filter], geom, geom2), compareOr(_: Option[Filter], dtg, dtg2))
        z3.strategies.head.secondary must beSome // secondary filter is too complex to compare here...
      }

      "with spatiotemporal and non-indexed attributes clauses" >> {
        val filter = and(geom, dtg, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containAllOf(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(f(geom))
        z2.strategies.head.secondary must beSome(and(dtg, nonIndexedAttr))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, dtg)
        z3.strategies.head.secondary must beSome(f(nonIndexedAttr))
      }

      "with spatiotemporal and indexed attributes clauses" >> {
        val filter = and(geom, dtg, indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(3)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must contain(Z2Index.name, Z3Index.name, AttributeIndex.name)
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(f(geom))
        z2.strategies.head.secondary must beSome(and(dtg, indexedAttr))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, dtg)
        z3.strategies.head.secondary must beSome(f(indexedAttr))
        val attr = options.find(_.strategies.head.index.name == AttributeIndex.name).get
        attr.strategies.head.primary must beSome(f(indexedAttr))
        attr.strategies.head.secondary must beSome(and(geom, dtg))
      }

      "with spatiotemporal, indexed and non-indexed attributes clauses" >> {
        val filter = and(geom, dtg, indexedAttr, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(3)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must contain(Z2Index.name, Z3Index.name, AttributeIndex.name)
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(f(geom))
        z2.strategies.head.secondary must beSome(and(dtg, indexedAttr, nonIndexedAttr))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, dtg)
        z3.strategies.head.secondary must beSome(and(indexedAttr, nonIndexedAttr))
        val attr = options.find(_.strategies.head.index.name == AttributeIndex.name).get
        attr.strategies.head.primary must beSome(f(indexedAttr))
        attr.strategies.head.secondary must beSome(and(geom, dtg, nonIndexedAttr))
      }

      "with spatiotemporal clauses and non-indexed attributes or" >> {
        val filter = and(f(geom), f(dtg), or(nonIndexedAttr, nonIndexedAttr2))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containAllOf(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(f(geom))
        z2.strategies.head.secondary must beSome(and(f(dtg), or(nonIndexedAttr, nonIndexedAttr2)))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, dtg)
        z3.strategies.head.secondary must beSome(or(nonIndexedAttr, nonIndexedAttr2))
      }

      "while ignoring world-covering geoms" >> {
        val filter = QueryPlanFilterVisitor(null, f(wholeWorld))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual includeStrategy
        options.head.strategies.head.primary must beNone
        options.head.strategies.head.secondary must beNone
      }

      "while ignoring world-covering geoms when other filters are present" >> {
        val filter = QueryPlanFilterVisitor(null, and(wholeWorld, geom, dtg))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containAllOf(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(f(geom))
        z2.strategies.head.secondary must beSome(f(dtg))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, dtg)
        z3.strategies.head.secondary must beNone
      }
    }

    "work for single clause filters" >> {
      "spatial" >> {
        val filter = f(geom)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual Z2Index.name
        options.head.strategies.head.primary must beSome(filter)
        options.head.strategies.head.secondary must beNone
      }

      "temporal" >> {
        val filter = f(dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual Z3Index.name
        options.head.strategies.head.primary must beSome(filter)
        options.head.strategies.head.secondary must beNone
      }

      "non-indexed attributes" >> {
        val filter = f(nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual includeStrategy
        options.head.strategies.head.primary must beNone
        options.head.strategies.head.secondary must beSome(filter)
      }

      "indexed attributes" >> {
        val filter = f(indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual AttributeIndex.name
        options.head.strategies.head.primary must beSome(filter)
        options.head.strategies.head.secondary must beNone
      }

      "low-cardinality attributes" >> {
        val filter = f(lowCardinaltiyAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual AttributeIndex.name
        options.head.strategies.head.primary must beSome(filter)
        options.head.strategies.head.secondary must beNone
      }

      "high-cardinality attributes" >> {
        val filter = f(highCardinaltiyAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual AttributeIndex.name
        options.head.strategies.head.primary must beSome(filter)
        options.head.strategies.head.secondary must beNone
      }
    }

    "work for simple ands" >> {
      "spatial" >> {
        val filter = and(geom, geom2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual Z2Index.name
        compareAnd(options.head.strategies.head.primary, geom, geom2)
        options.head.strategies.head.secondary must beNone
      }

      "temporal" >> {
        val filter = and(dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual Z3Index.name
        compareAnd(options.head.strategies.head.primary, dtg, dtgOverlap)
        options.head.strategies.head.secondary must beNone
      }

      "non-indexed attributes" >> {
        val filter = and(nonIndexedAttr, nonIndexedAttr2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual includeStrategy
        options.head.strategies.head.primary must beNone
        options.head.strategies.head.secondary must beSome(filter)
      }

      "indexed attributes" >> {
        val filter = and(indexedAttr, indexedAttr2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual AttributeIndex.name
        options.head.strategies.head.primary must beSome(filter)
        options.head.strategies.head.secondary must beNone
      }

      "low-cardinality attributes" >> {
        val filter = and(lowCardinaltiyAttr, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual AttributeIndex.name
        options.head.strategies.head.primary must beSome(f(lowCardinaltiyAttr))
        options.head.strategies.head.secondary must beSome(f(nonIndexedAttr))
      }
    }

    "split filters on OR" >> {
      "with spatiotemporal clauses" >> {
        val filter = or(geom, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(2)
        val z3 = options.head.strategies.find(_.index.name == Z3Index.name)
        z3 must beSome
        z3.get.primary must beSome(f(dtg))
        z3.get.secondary must beSome(not(geom))
        val st = options.head.strategies.find(_.index.name == Z2Index.name)
        st must beSome
        st.get.primary must beSome(f(geom))
        st.get.secondary must beNone
      }

      "with multiple spatial clauses" >> {
        val filter = or(geom, geom2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(1)
        options.head.strategies.head.index.name mustEqual Z2Index.name
        compareOr(options.head.strategies.head.primary, geom, geom2)
        options.head.strategies.head.secondary must beNone
      }

      "with spatiotemporal and indexed attribute clauses" >> {
        val filter = or(geom, indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(2)
        options.head.strategies.map(_.index.name) must containTheSameElementsAs(Seq(Z2Index.name, AttributeIndex.name))
        options.head.strategies.find(_.index.name == Z2Index.name).get.primary must beSome(f(geom))
        options.head.strategies.find(_.index.name == Z2Index.name).get.secondary must beNone
        options.head.strategies.find(_.index.name == AttributeIndex.name).get.primary must beSome(f(indexedAttr))
        options.head.strategies.find(_.index.name == AttributeIndex.name).get.secondary must beSome(not(geom))
      }

      "and collapse overlapping query filters" >> {
        "with spatiotemporal and non-indexed attribute clauses" >> {
          val filter = or(geom, nonIndexedAttr)
          val options = splitter.getQueryOptions(filter)
          options must haveLength(1)
          options.head.strategies must haveLength(1)
          options.head.strategies.head.index.name mustEqual includeStrategy
          options.head.strategies.head.primary must beNone
          options.head.strategies.head.secondary must beSome(filter)
        }
      }
    }

    "split nested filters" >> {
      "with ANDs" >> {
        val filter = and(f(geom), and(dtg, nonIndexedAttr))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must containAllOf(Seq(Z2Index.name, Z3Index.name))
        val z2 = options.find(_.strategies.head.index.name == Z2Index.name).get
        z2.strategies.head.primary must beSome(f(geom))
        z2.strategies.head.secondary must beSome(and(dtg, nonIndexedAttr))
        val z3 = options.find(_.strategies.head.index.name == Z3Index.name).get
        compareAnd(z3.strategies.head.primary, geom, dtg)
        z3.strategies.head.secondary must beSome(f(nonIndexedAttr))
      }

      "with ORs" >> {
        val filter = or(f(geom), or(dtg, indexedAttr))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.strategies must haveLength(3)
        options.head.strategies.map(_.index.name) must
            containTheSameElementsAs(Seq(Z3Index.name, Z2Index.name, AttributeIndex.name))
        options.head.strategies.map(_.primary) must contain(beSome(f(geom)), beSome(f(dtg)), beSome(f(indexedAttr)))
        options.head.strategies.map(_.secondary) must contain(beSome(not(geom)), beSome(not(geom, indexedAttr)))
        options.head.strategies.map(_.secondary) must contain(beNone)
      }
    }

    "support indexed date attributes" >> {
      val sft = SimpleFeatureTypes.createType("dtgIndex", "dtg:Date:index=full,*geom:Point:srid=4326")
      sft.setIndices(GeoMesaFeatureIndexFactory.indices(sft))
      val splitter = new FilterSplitter(sft, GeoMesaFeatureIndexFactory.create(null, sft, sft.getIndices), None)
      val filter = f("dtg TEQUALS 2014-01-01T12:30:00.000Z")
      val options = splitter.getQueryOptions(filter)
      options must haveLength(2)
      foreach(options)(_.strategies must haveLength(1))
      val strategies = options.map(_.strategies.head)
      strategies.map(_.index.name) must containTheSameElementsAs(Seq(AttributeIndex.name, Z3Index.name))
      foreach(strategies) { strategy =>
        strategy.primary must beSome(filter)
        strategy.secondary must beNone
      }
    }

    "provide only one option on OR queries of high cardinality indexed attributes" >> {
      def testHighCard(attrPart: String): MatchResult[Any] = {
        val bbox = "BBOX(geom, 40.0,40.0,50.0,50.0)"
        val dtg  = "dtg DURING 2014-01-01T00:00:00+00:00/2014-01-01T23:59:59+00:00"
        val filter = f(s"($attrPart) AND $bbox AND $dtg")
        val options = splitter.getQueryOptions(filter)
        options must haveLength(3)

        forall(options)(_.strategies must haveLength(1))
        options.map(_.strategies.head.index.name) must
            containTheSameElementsAs(Seq(AttributeIndex.name, Z2Index.name, Z3Index.name))

        val attrQueryFilter = options.find(_.strategies.head.index.name == AttributeIndex.name).get.strategies.head
        compareOr(attrQueryFilter.primary, decomposeOr(f(attrPart)): _*)
        compareAnd(attrQueryFilter.secondary, bbox, dtg)

        val z2QueryFilters = options.find(_.strategies.head.index.name == Z2Index.name).get.strategies.head
        z2QueryFilters.primary must beSome(f(bbox))
        z2QueryFilters.secondary must beSome(beAnInstanceOf[And])
        val z2secondary = z2QueryFilters.secondary.get.asInstanceOf[And].getChildren.toSeq
        z2secondary must haveLength(2)
        z2secondary must contain(beAnInstanceOf[Or], beAnInstanceOf[During])
        compareOr(z2secondary.find(_.isInstanceOf[Or]), decomposeOr(f(attrPart)): _*)
        z2secondary.find(_.isInstanceOf[During]).get mustEqual f(dtg)

        val z3QueryFilters = options.find(_.strategies.head.index.name == Z3Index.name).get.strategies.head
        compareAnd(z3QueryFilters.primary, bbox, dtg)
        compareOr(z3QueryFilters.secondary, decomposeOr(f(attrPart)): _*)
      }

      val orQuery = (0 until 5).map( i => s"high = 'h$i'").mkString(" OR ")
      val inQuery = s"high in (${(0 until 5).map( i => s"'h$i'").mkString(",")})"
      Seq(orQuery, inQuery).forall(testHighCard)
    }

    "extract the simple parts from complex filters" >> {
      val bbox1 = "bbox(geom, -120, 45, -121, 46)"
      val bbox2 = "bbox(geom, -120, 48, -121, 49)"
      val bboxOr = s"($bbox1 OR $bbox2)"
      val attribute1 = "(attr1 IN ('foo', 'bar') AND attr3 IS NOT NULL AND attr4 IN ('qaz', 'wsx'))"
      val attribute2 = "(attr1 IN ('baz', 'jim') AND attr3 IS NOT NULL AND attr4 IN ('qaz', 'wsx'))"
      val filter = f(s"$bboxOr AND ($attribute1 OR $attribute2)")
      val options = splitter.getQueryOptions(filter)
      options must haveLength(1)
      options.head.strategies must haveLength(1)
      options.head.strategies.head.index.name mustEqual Z2Index.name
      compareOr(options.head.strategies.head.primary, bbox1, bbox2)
      options.head.strategies.head.secondary must beSome // note: filter is too complex to compare explicitly...
    }
  }
}
