/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.util.Date

import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators}
import org.apache.parquet.io.api.Binary
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class FilterConverterTest extends Specification with AllExpectations {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  def convert(filter: String): (Option[FilterPredicate], Option[Filter]) =
    FilterConverter.convert(sft, ECQL.toFilter(filter))

  def flatten(and: Operators.And): Seq[FilterPredicate] = {
    val remaining = scala.collection.mutable.Queue[FilterPredicate](and)
    val result = Seq.newBuilder[FilterPredicate]
    while (remaining.nonEmpty) {
      remaining.dequeue() match {
        case a: Operators.And => remaining ++= Seq(a.getLeft, a.getRight)
        case f => result += f
      }
    }
    result.result()
  }

  "FilterConverter" should {
    "convert geo filter to min/max x/y" >> {
      val (pFilter, gFilter) = convert("bbox(geom, -24.0, -25.0, -18.0, -19.0)")
      gFilter must beNone
      pFilter must beSome(beAnInstanceOf[Operators.And])
      val clauses = flatten(pFilter.get.asInstanceOf[Operators.And])
      clauses must haveLength(4)

      val xmin = clauses.collectFirst {
        case c: Operators.GtEq[java.lang.Double] if c.getColumn.getColumnPath.toDotString == "geom.x" => c
      }
      val ymin = clauses.collectFirst {
        case c: Operators.GtEq[java.lang.Double] if c.getColumn.getColumnPath.toDotString == "geom.y" => c
      }
      val xmax = clauses.collectFirst {
        case c: Operators.LtEq[java.lang.Double] if c.getColumn.getColumnPath.toDotString == "geom.x" => c
      }
      val ymax = clauses.collectFirst {
        case c: Operators.LtEq[java.lang.Double] if c.getColumn.getColumnPath.toDotString == "geom.y" => c
      }

      xmin.map(_.getValue.doubleValue()) must beSome(-24.0)
      ymin.map(_.getValue.doubleValue()) must beSome(-25.0)
      xmax.map(_.getValue.doubleValue()) must beSome(-18.0)
      ymax.map(_.getValue.doubleValue()) must beSome(-19.0)
    }

    "convert dtg ranges to long ranges" >> {
      val (pFilter, gFilter) = convert("dtg BETWEEN '2017-01-01T00:00:00.000Z' AND '2017-01-05T00:00:00.000Z'")
      gFilter must beNone
      pFilter must beSome(beAnInstanceOf[Operators.And])
      val clauses = flatten(pFilter.get.asInstanceOf[Operators.And])
      clauses must haveLength(2)

      val lt = clauses.collectFirst {
        case c: Operators.LtEq[java.lang.Long] if c.getColumn.getColumnPath.toDotString == "dtg" => c
      }
      val gt = clauses.collectFirst {
        case c: Operators.GtEq[java.lang.Long] if c.getColumn.getColumnPath.toDotString == "dtg" => c
      }

      lt.map(_.getValue.longValue()) must beSome(Converters.convert("2017-01-05T00:00:00.000Z", classOf[Date]).getTime)
      gt.map(_.getValue.longValue()) must beSome(Converters.convert("2017-01-01T00:00:00.000Z", classOf[Date]).getTime)
    }

    "augment property equals column" >> {
      val (pFilter, gFilter) =
        convert("name = 'foo' AND dtg BETWEEN '2017-01-01T00:00:00.000Z' AND '2017-01-05T00:00:00.000Z'")
      gFilter must beNone
      pFilter must beSome(beAnInstanceOf[Operators.And])
      val clauses = flatten(pFilter.get.asInstanceOf[Operators.And])
      clauses must haveLength(3)

      val eq = clauses.collectFirst {
        case c: Operators.Eq[Binary] if c.getColumn.getColumnPath.toDotString == "name" => c
      }
      val lt = clauses.collectFirst {
        case c: Operators.LtEq[java.lang.Long] if c.getColumn.getColumnPath.toDotString == "dtg" => c
      }
      val gt = clauses.collectFirst {
        case c: Operators.GtEq[java.lang.Long] if c.getColumn.getColumnPath.toDotString == "dtg" => c
      }

      eq.map(_.getValue) must beSome(Binary.fromString("foo"))
      lt.map(_.getValue.longValue()) must beSome(Converters.convert("2017-01-05T00:00:00.000Z", classOf[Date]).getTime)
      gt.map(_.getValue.longValue()) must beSome(Converters.convert("2017-01-01T00:00:00.000Z", classOf[Date]).getTime)
    }

    "query with an int" >> {
      val (pFilter, gFilter) = convert("age = 20")
      gFilter must beNone
      pFilter must beSome(beAnInstanceOf[Operators.Eq[java.lang.Integer]])
      pFilter.get.asInstanceOf[Operators.Eq[java.lang.Integer]].getColumn.getColumnPath.toDotString mustEqual "age"
      pFilter.get.asInstanceOf[Operators.Eq[java.lang.Integer]].getValue.intValue() mustEqual 20
    }
  }
}
