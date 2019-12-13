/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduFilter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class KuduSimpleFeatureSchemaTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("test",
    "string:String,int:Int,long:Long,float:Float,double:Double,boolean:Boolean,date:Date,uuid:UUID," +
        "bytes:Bytes,list:List[Float],map:Map[String,Int],*pt:Point:srid=4326,ls:LineString:srid=4326")

  val schema = KuduSimpleFeatureSchema(sft)

  "KuduSimpleFeatureSchema" should {
    "have adapters for each attribute" in {
      schema.adapters must haveLength(sft.getAttributeCount)
    }

    "serialize features" in {
      val sf = ScalaSimpleFeature.create(sft, "id0", "string0", 1, 1L, 1f, 1d, true, "2018-01-02T00:01:30.000Z",
        "e8c274ef-7d7c-4556-af68-1fc4b7f26a36", Array[Byte](0, 1, 2), Seq(0f, 1f, 2f).asJava,
        Map("zero" -> 0, "one" -> 1, "two" -> 2).asJava, "POINT(45 55)", "LINESTRING(45 55, 46 56, 47 57)")
      foreach(sf.getAttributes.asScala)(_ must not(beNull))

      val values = schema.serialize(sf)
      values must haveLength(sft.getAttributeCount)

      foreach(values.map(_.value).zip(sf.getAttributes.asScala)) { case (v, a) => v mustEqual a}
    }

    "extract equality predicates" in {
      val filters = Seq("string = 'foo'", "int = 4", "long = 5", "float = 2.0", "double = 3.0",
        "date = '2018-01-02T00:01:30.000Z'", "uuid = 'e8c274ef-7d7c-4556-af68-1fc4b7f26a36'")
      foreach(filters) { filter =>
        val KuduFilter(predicates, ecql) = schema.predicate(ECQL.toFilter(filter))
        ecql must beNone
        predicates must haveLength(1)
        predicates.head.toPB.hasEquality must beTrue
      }
    }

    "extract OR equality predicates" in {
      val filters = Seq("string IN ('foo1', 'foo2')", "int IN (4, 5)", "long IN (5, 6)", "float IN (2.0, 2.1)",
        "double IN (3.0, 3.1)", "uuid IN ('e8c274ef-7d7c-4556-af68-1fc4b7f26a36', 'e8c274ef-7d7c-4556-af68-1fc4b7f26a37')")
      foreach(filters) { filter =>
        val KuduFilter(predicates, ecql) = schema.predicate(ECQL.toFilter(filter))
        ecql must beNone
        predicates must haveLength(1)
        predicates.head.toPB.hasInList must beTrue
      }
    }

    "extract bbox predicates" in {
      val KuduFilter(predicates, ecql) = schema.predicate(ECQL.toFilter("bbox(pt,-120,45,-110,55)"))
      ecql must beNone
      predicates must haveLength(4) // x min, y min, x max, y max
      forall(predicates)(_.toPB.hasRange must beTrue)
    }

    "extract intersects predicates" in {
      val filter = ECQL.toFilter("intersects(pt,'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')")
      val KuduFilter(predicates, ecql) = schema.predicate(filter)
      ecql must beSome(filter)
      predicates must haveLength(4) // x min, y min, x max, y max
      forall(predicates)(_.toPB.hasRange must beTrue)
    }
  }
}
