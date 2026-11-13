/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.decomposeAnd
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource.MergedSourceSft
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date

class MergedFilterProviderTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createImmutableType("test",
    "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.index.dtg=dtg")

  def compareFilters(actual: Filter, expected: Filter): MatchResult[_] =
    decomposeAnd(FastFilterFactory.optimize(sft, actual)) must
      containTheSameElementsAs(decomposeAnd(FastFilterFactory.optimize(sft, expected)))

  "MergedFilterProvider" should {
    "calculate date offset filters" in {
      val now = Instant.now()
      val ev = new MergedSourceSft[AnyRef]() {
        override def sft(source: AnyRef, typeName: String): SimpleFeatureType = MergedFilterProviderTest.this.sft
      }
      def getFilter(f: String, s: String, e: String, merged: Filter = Filter.INCLUDE): Filter = {
        val providers = Option(f).map(ECQL.toFilter).map(MergedFilterProvider.static).toSeq ++
          Option(s).map(MergedFilterProvider.from) ++ Option(e).map(MergedFilterProvider.to)
        MergedSource[AnyRef](null, MergedFilterProvider.and(providers)).filter(sft.getTypeName, QueryContext(merged, now))(ev)
      }

      compareFilters(
        getFilter("name = 'alice'", "2022-02-01T00:00:00.000Z", "2022-02-02T00:00:00.000Z"),
        ECQL.toFilter("name = 'alice' AND dtg >= '2022-02-01T00:00:00.000Z' AND dtg < '2022-02-02T00:00:00.000Z'")
      )
      compareFilters(
        getFilter(null, "2022-02-01T00:00:00.000Z", "2022-02-02T00:00:00.000Z"),
        ECQL.toFilter("dtg >= '2022-02-01T00:00:00.000Z' AND dtg < '2022-02-02T00:00:00.000Z'")
      )
      compareFilters(
        getFilter(null, "-P1D", null),
        ECQL.toFilter(s"dtg >= '${DateParsing.formatInstant(now.minus(1, ChronoUnit.DAYS))}'")
      )
      compareFilters(
        getFilter(null, null, "-P1D"),
        ECQL.toFilter(s"dtg < '${DateParsing.formatInstant(now.minus(1, ChronoUnit.DAYS))}'")
      )
      compareFilters(
        getFilter("dtg <= testDate()", null, null, ECQL.toFilter("dtg = testDate()")),
        // note: simplification isn't performed by the merged view
        ECQL.toFilter(Seq.fill(2)(DateParsing.formatDate(MergedFilterProviderTest.response)).mkString("dtg <= '", "' AND dtg = '", "'"))
      )
      MergedFilterProviderTest.response = new Date(MergedFilterProviderTest.response.getTime + 10000)
      compareFilters(
        getFilter("dtg <= testDate()", null, null, ECQL.toFilter("dtg = testDate()")),
        // note: simplification isn't performed by the merged view
        ECQL.toFilter(Seq.fill(2)(DateParsing.formatDate(MergedFilterProviderTest.response)).mkString("dtg <= '", "' AND dtg = '", "'"))
      )
    }

    "pass the same context through composed and typed providers" in {
      val base = ECQL.toFilter("age > 20")
      val context = QueryContext(base)
      val seen = scala.collection.mutable.ArrayBuffer.empty[QueryContext]
      def recording(value: String): MergedFilterProvider = new MergedFilterProvider {
        override def init(config: Map[String, String]): Unit = {}
        override def apply(sft: SimpleFeatureType, query: QueryContext): Option[Filter] = {
          seen += query
          query.state.put(value, query.base)
          None
        }
        override def close(): Unit = {}
      }
      val typed = MergedFilterProvider.TypedProvider(Map(sft.getTypeName -> recording("typed")), None)
      MergedFilterProvider.and(Seq(recording("first"), typed)).apply(sft, context) must beNone
      seen must haveLength(2)
      foreach(seen)(query => (query eq context) must beTrue)
      context.state.get("first") mustEqual base
      context.state.get("typed") mustEqual base
    }

    "use the context base filter to suppress disjoint date intervals" in {
      val context = QueryContext(ECQL.toFilter("dtg < '2021-01-01T00:00:00Z'"))
      MergedFilterProvider.from("2022-01-01T00:00:00Z").apply(sft, context) must beSome(Filter.EXCLUDE)
    }

    "parse date offset filters" in {
      MergedFilterProvider.from("2022-02-01T00:00:00.000Z").getClass.getSimpleName mustEqual "FromDateProvider"
      MergedFilterProvider.from("2022-02-01Z").getClass.getSimpleName mustEqual "FromDateProvider"
      MergedFilterProvider.from("-P1D").getClass.getSimpleName mustEqual "FromOffsetProvider"
      MergedFilterProvider.from("foo") must throwAn[IllegalArgumentException]
    }
  }
}

object MergedFilterProviderTest {

  private var response = new Date()

  class TestFunction extends FunctionExpressionImpl(new FunctionNameImpl("testDate", classOf[java.util.Date])) {
    override def evaluate(o: AnyRef): AnyRef = response
  }

}
