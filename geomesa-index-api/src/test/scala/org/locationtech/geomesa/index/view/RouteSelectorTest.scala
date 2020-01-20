/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data.{DataStore, Query}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RouteSelectorTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

  "RouteSelector" should {
    "route queries based on filter attributes" in {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

      val configs = Seq.tabulate(3) { i =>
        val map = new java.util.HashMap[String, AnyRef]
        if (i == 0) {
          map.put(RouteSelectorByAttribute.RouteAttributes, Seq("id", Seq.empty.asJava).asJava)
        } else if (i == 1) {
          map.put(RouteSelectorByAttribute.RouteAttributes, Seq(Seq("geom", "dtg").asJava, Seq("geom").asJava).asJava)
        } else if (i == 2) {
          map.put(RouteSelectorByAttribute.RouteAttributes, Seq("name", Seq("age", "dtg").asJava).asJava)
        }
        mock[DataStore] -> map
      }
      val stores = configs.map(_._1)

      val router = new RouteSelectorByAttribute()
      router.init(configs)

      def query(filter: String) = new Query(sft.getTypeName, ECQL.toFilter(filter))

      val s1 = Seq("INCLUDE", "IN ('1', '2')", "foo = 'bar'", "age = 21")
      foreach(s1)(f => router.route(sft, query(f)) must beSome(stores.head))

      val s2 = Seq(
        "bbox(geom,-75,-45,-45,-30)",
        "dtg during 2019-01-01T00:00:00.000Z/2019-01-01T01:00:00.000Z and bbox(geom,-180,-90,180,90)")
      foreach(s2)(f => router.route(sft, query(f)) must beSome(stores(1)))

      val s3 = Seq(
        "name = 'foo'",
        "age = 21 AND dtg during 2019-01-01T00:00:00.000Z/2019-01-01T01:00:00.000Z",
        "name = 'foo' AND dtg during 2019-01-01T00:00:00.000Z/2019-01-01T01:00:00.000Z")
      foreach(s3)(f => router.route(sft, query(f)) must beSome(stores(2)))
    }
  }
}
