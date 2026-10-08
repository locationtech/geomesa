/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import org.geotools.api.filter.Filter
import org.geotools.data.DataUtilities
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.visitor.SimplifyingFilterVisitor
import org.junit.runner.RunWith
import org.locationtech.jts.io.WKTReader
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Collections

@RunWith(classOf[JUnitRunner])
class SpatialIndexFunctionTest extends Specification {

  private val filters = CommonFactoryFinder.getFilterFactory
  private val geom = new WKTReader().read("POINT (10 10)")
  private val schema = DataUtilities.createType("test", "*geom:Point:srid=4326")
  private val feature = SimpleFeatureBuilder.build(schema, Array[AnyRef](geom), "id")

  "Spatial index functions" should {
    "require a geometry argument" in {
      foreach(Seq(new Z2Function(), new XZ2Function())) { function =>
        function.setParameters(Collections.emptyList()) must throwAn[IllegalArgumentException]
      }
    }

    "evaluate literal geometries without a feature and fold into constants" in {
      foreach(Seq("z2", "xz2")) { name =>
        val function = filters.function(name, filters.literal(geom))
        val value = function.evaluate(feature)
        value must not(beNull)
        function.evaluate(null) mustEqual value
        val filter = filters.equals(function, filters.literal(value))
        filter.accept(new SimplifyingFilterVisitor(), null) mustEqual Filter.INCLUDE
        filters.equals(function, filters.literal("different")).accept(new SimplifyingFilterVisitor(), null) mustEqual Filter.EXCLUDE
      }
    }

    "retain explicit property dependencies during simplification" in {
      foreach(Seq("z2", "xz2")) { name =>
        val function = filters.function(name, filters.property("geom"))
        val filter = filters.equals(function, filters.literal(function.evaluate(feature)))
        val visitor = new SimplifyingFilterVisitor()
        visitor.setFeatureType(schema)
        val simplified = filter.accept(visitor, null).asInstanceOf[Filter]
        simplified must not(beEqualTo(Filter.INCLUDE))
        simplified must not(beEqualTo(Filter.EXCLUDE))
        simplified.evaluate(feature) must beTrue
      }
    }

    "return null for null geometry arguments" in {
      foreach(Seq("z2", "xz2")) { name =>
        filters.function(name, filters.literal(null)).evaluate(null) must beNull
        filters.function(name, filters.property("geom")).evaluate(null) must beNull
      }
    }
  }
}
