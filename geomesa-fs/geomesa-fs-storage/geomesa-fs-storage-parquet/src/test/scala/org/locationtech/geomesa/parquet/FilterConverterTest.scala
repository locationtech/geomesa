/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.util.Date

import org.apache.parquet.filter2.predicate.Operators
import org.geotools.factory.CommonFactoryFinder
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class FilterConverterTest extends Specification with AllExpectations {

  sequential

  "FilterConverter" should {
    val ff = CommonFactoryFinder.getFilterFactory2
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val conv = new FilterConverter(sft)

    "convert geo filter to min/max x/y" >> {
      val pfilter = conv.convert(ff.bbox("geom", -24.0, -25.0, -18.0, -19.0, "EPSG:4326"))._1.get
      pfilter must beAnInstanceOf[Operators.And]
      // TODO extract the rest of the AND filters to test
      val last = pfilter.asInstanceOf[Operators.And].getRight.asInstanceOf[Operators.LtEq[java.lang.Double]]
      last.getValue mustEqual -19.0
      last.getColumn.getColumnPath.toDotString mustEqual "geom.y"
    }

    "convert dtg ranges to long ranges" >> {
      val pfilter = conv.convert(ff.between(ff.property("dtg"), ff.literal("2017-01-01T00:00:00.000Z"), ff.literal("2017-01-05T00:00:00.000Z")))._1.get
      pfilter must beAnInstanceOf[Operators.And]
      val and = pfilter.asInstanceOf[Operators.And]
      and.getLeft.asInstanceOf[Operators.GtEq[java.lang.Long]].getColumn.getColumnPath.toDotString mustEqual "dtg"
      and.getLeft.asInstanceOf[Operators.GtEq[java.lang.Long]].getValue mustEqual Converters.convert("2017-01-01T00:00:00.000Z", classOf[Date]).getTime

      and.getRight.asInstanceOf[Operators.LtEq[java.lang.Long]].getColumn.getColumnPath.toDotString mustEqual "dtg"
      and.getRight.asInstanceOf[Operators.LtEq[java.lang.Long]].getValue mustEqual Converters.convert("2017-01-05T00:00:00.000Z", classOf[Date]).getTime
    }

    "ignore dtg column for now for filter augmentation" >> {
      val f = ff.between(ff.property("dtg"), ff.literal("2017-01-01T00:00:00.000Z"), ff.literal("2017-01-05T00:00:00.000Z"))
      val res = conv.convert(f)
      res._2 mustEqual f
    }

    "augment property equals column" >> {
      import scala.collection.JavaConversions._
      val f = ff.and(List[org.opengis.filter.Filter](
        ff.greaterOrEqual(ff.property("dtg"), ff.literal("2017-01-01T00:00:00.000Z")),
        ff.lessOrEqual(ff.property("dtg"), ff.literal("2017-01-05T00:00:00.000Z")),
        ff.equals(ff.property("name"), ff.literal("foo"))))
      val res = conv.convert(f)
      val expectedAug = ff.and(
        List[org.opengis.filter.Filter](
          ff.greaterOrEqual(ff.property("dtg"), ff.literal("2017-01-01T00:00:00.000Z")),
          ff.lessOrEqual(ff.property("dtg"), ff.literal("2017-01-05T00:00:00.000Z")),
          org.opengis.filter.Filter.INCLUDE))
      res._2 mustEqual expectedAug
    }

    "query with an int" >> {
      val f = ff.equals(ff.property("age"), ff.literal(20))
      val res = conv.convert(f)
      success
    }
  }
}
