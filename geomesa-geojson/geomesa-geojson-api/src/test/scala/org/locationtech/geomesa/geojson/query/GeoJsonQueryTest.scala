/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geojson.query

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.PathAttribute
import org.locationtech.geomesa.geojson.GeoMesaIndexPropertyTransformer
import org.locationtech.geomesa.geojson.query.GeoJsonQuery._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoJsonQueryTest extends Specification {

  "GeoJsonQuery" should {
    "parse json predicates" in {
      GeoJsonQuery("""{"status":"A"}""") mustEqual Equals("status", "A")
      GeoJsonQuery("""{"status":"A","age":{"$lt":30}}""") mustEqual
        And(Equals("status", "A"), LessThan("age", 30, inclusive = false))
      GeoJsonQuery("""{"$or":[{"status":"A"},{"age":{"$lte":30}}]}""") mustEqual
        Or(Equals("status", "A"), LessThan("age", 30, inclusive = true))
      GeoJsonQuery("""{"loc":{"$within":{"$geometry":{"type":"Polygon","coordinates":[[[0,0],[3,6],[6,1],[0,0]]]}}}}""") mustEqual
        Within("loc", WKTUtils.read("POLYGON ((0 0, 3 6, 6 1, 0 0))"))
      GeoJsonQuery("""{"loc":{"$bbox":[-180,-90.0,180,90.0]}}""") mustEqual
        Bbox("loc", -180.0, -90.0, 180.0, 90.0)
    }

    "unparse json predicates" in {
      val queries = Seq(
        """{"status":"A"}""",
        """{"status":"A","age":{"$lt":30}}""",
        """{"$or":[{"status":"A"},{"age":{"$lte":30}}]}""",
        """{"loc":{"$within":{"$geometry":{"type":"Polygon","coordinates":[[[0.1,0.1],[3.1,6.1],[6.1,1.1],[0.1,0.1]]]}}}}""",
        """{"loc":{"$bbox":[-180.0,-90.0,180.0,90.0]}}"""
      )
      forall(queries) { q => GeoJsonQuery(q).toString mustEqual q }
    }

    "apply" in {
      val geom = WKTUtils.read("POINT (10 10)")
      GeoJsonQuery.Include mustEqual GeoJsonQuery.Include
      GeoJsonQuery.LessThan("age", 30, inclusive = false) mustEqual LessThan("age", 30, inclusive = false)
      GeoJsonQuery.GreaterThan("age", 30, inclusive = false) mustEqual GreaterThan("age", 30, inclusive = false)
      GeoJsonQuery.Bbox(-10, -20, 10, 20) mustEqual Bbox(GeoJsonQuery.defaultGeom, -10, -20, 10, 20)
      GeoJsonQuery.Contains(geom) mustEqual Contains(GeoJsonQuery.defaultGeom, geom)
      GeoJsonQuery.Within(geom) mustEqual Within(GeoJsonQuery.defaultGeom, geom)
      GeoJsonQuery.Intersects(geom) mustEqual Intersects(GeoJsonQuery.defaultGeom, geom)
    }

    "translate to CQL" in {
      ECQL.toCQL(GeoJsonQuery("""{"id":"foo"}""").toFilter(new GeoMesaIndexPropertyTransformer(None, None))) mustEqual """"$.json.id" = 'foo'"""
      ECQL.toCQL(GeoJsonQuery("""{"id":"foo"}""").toFilter(new GeoMesaIndexPropertyTransformer(Some(Seq(PathAttribute("id"))), None))) mustEqual "IN ('foo')"
      ECQL.toCQL(GeoJsonQuery("""{"loc":{"$bbox":[-180,-90.0,180,90.0]}}""").toFilter(new GeoMesaIndexPropertyTransformer(None, None))) mustEqual
        "BBOX($.json.loc, -180.0,-90.0,180.0,90.0)" // TODO this won't work with non-default geoms due to CQL parsing...
      ECQL.toCQL(GeoJsonQuery("""{"geometry":{"$bbox":[-180,-90.0,180,90.0]}}""").toFilter(new GeoMesaIndexPropertyTransformer(None, None))) mustEqual
        "BBOX(geom, -180.0,-90.0,180.0,90.0)"
    }
  }
}
