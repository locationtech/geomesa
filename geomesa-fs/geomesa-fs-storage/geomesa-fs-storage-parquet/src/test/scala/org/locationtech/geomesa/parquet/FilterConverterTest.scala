/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators}
import org.apache.parquet.io.api.Binary
import org.geotools.api.filter.Filter
import org.geotools.api.filter.spatial.BBOX
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.parquet.FilterConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import java.util.Date

@RunWith(classOf[JUnitRunner])
class FilterConverterTest extends Specification with AllExpectations {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326,line:LineString:srid=4326")

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

  def flatten(or: Operators.Or): Seq[FilterPredicate] = {
    val remaining = scala.collection.mutable.Queue[FilterPredicate](or)
    val result = Seq.newBuilder[FilterPredicate]
    while (remaining.nonEmpty) {
      remaining.dequeue() match {
        case a: Operators.Or => remaining ++= Seq(a.getLeft, a.getRight)
        case f => result += f
      }
    }
    result.result()
  }

  "FilterConverter" should {
    "convert point geo filter to min/max x/y" >> {
      val (pFilter, gFilter) = convert("bbox(geom, -24.0, -25.0, -18.0, -19.0)")
      gFilter must beSome(beAnInstanceOf[BBOX])
      pFilter must beSome(beAnInstanceOf[Operators.Or])

      // bbox OR x/y
      val orClauses = flatten(pFilter.get.asInstanceOf[Operators.Or])
      orClauses must haveLength(2)

      val andClauses = orClauses.collect { case and: Operators.And => flatten(and) }
      andClauses must haveLength(2)

      val bbox = andClauses.find(_.exists(_.toString.contains("__"))).orNull
      bbox must not(beNull)
      bbox must haveLength(4)

      bbox.collectFirst {
        case c: Operators.LtEq[java.lang.Float] if c.getColumn.getColumnPath.toDotString == "__geom_bbox__.xmin" => c.getValue.floatValue()
      } must beSome(-18.0f)
      bbox.collectFirst {
        case c: Operators.LtEq[java.lang.Float] if c.getColumn.getColumnPath.toDotString == "__geom_bbox__.ymin" => c.getValue.floatValue()
      } must beSome(-19.0f)
      bbox.collectFirst {
        case c: Operators.GtEq[java.lang.Float] if c.getColumn.getColumnPath.toDotString == "__geom_bbox__.xmax" => c.getValue.floatValue()
      } must beSome(-24.0f)
      bbox.collectFirst {
        case c: Operators.GtEq[java.lang.Float] if c.getColumn.getColumnPath.toDotString == "__geom_bbox__.ymax" => c.getValue.floatValue()
      } must beSome(-25.0f)

      val xy = andClauses.find(_ != bbox).orNull
      xy must not(beNull)

      xy must haveLength(4)

      xy.collectFirst {
        case c: Operators.GtEq[java.lang.Double] if c.getColumn.getColumnPath.toDotString == "geom.x" => c.getValue.doubleValue()
      } must beSome(-24.0)
      xy.collectFirst {
        case c: Operators.GtEq[java.lang.Double] if c.getColumn.getColumnPath.toDotString == "geom.y" => c.getValue.doubleValue()
      } must beSome(-25.0)
      xy.collectFirst {
        case c: Operators.LtEq[java.lang.Double] if c.getColumn.getColumnPath.toDotString == "geom.x" => c.getValue.doubleValue()
      } must beSome(-18.0)
      xy.collectFirst {
        case c: Operators.LtEq[java.lang.Double] if c.getColumn.getColumnPath.toDotString == "geom.y" => c.getValue.doubleValue()
      } must beSome(-19.0)
    }

    "convert non-point geo filter to bbox x/y" >> {
      val (pFilter, gFilter) = convert("bbox(line, -24.0, -25.0, -18.0, -19.0)")
      gFilter must beSome(beAnInstanceOf[BBOX])
      pFilter must beSome(beAnInstanceOf[Operators.Or])

      val orClauses = flatten(pFilter.get.asInstanceOf[Operators.Or])
      orClauses must haveLength(2)

      val nullCheck = orClauses.collectFirst { case c: Operators.Eq[java.lang.Float] => c }
      nullCheck must beSome
      nullCheck.get.getValue must beNull
      nullCheck.get.getColumn.getColumnPath.toList.size() mustEqual 2
      nullCheck.get.getColumn.getColumnPath.toList.get(0) mustEqual "__line_bbox__"

      val bbox = orClauses.collectFirst { case a: Operators.And => a }
      bbox must beSome

      val clauses = flatten(bbox.get)
      clauses must haveLength(4)

      val xmin = clauses.collectFirst {
        case c: Operators.LtEq[java.lang.Float] if c.getColumn.getColumnPath.toDotString == "__line_bbox__.xmin" => c
      }
      val ymin = clauses.collectFirst {
        case c: Operators.LtEq[java.lang.Float] if c.getColumn.getColumnPath.toDotString == "__line_bbox__.ymin" => c
      }
      val xmax = clauses.collectFirst {
        case c: Operators.GtEq[java.lang.Float] if c.getColumn.getColumnPath.toDotString == "__line_bbox__.xmax" => c
      }
      val ymax = clauses.collectFirst {
        case c: Operators.GtEq[java.lang.Float] if c.getColumn.getColumnPath.toDotString == "__line_bbox__.ymax" => c
      }

      xmin.map(_.getValue.floatValue()) must beSome(-18.0f)
      ymin.map(_.getValue.floatValue()) must beSome(-19.0f)
      xmax.map(_.getValue.floatValue()) must beSome(-24.0f)
      ymax.map(_.getValue.floatValue()) must beSome(-25.0f)
    }

    "convert dtg ranges to long ranges" >> {
      val (pFilter, gFilter) = convert("dtg BETWEEN '2017-01-01T00:00:00.000Z' AND '2017-01-05T00:00:00.000Z'")
      gFilter must beNone
      pFilter must beSome(beAnInstanceOf[Operators.Or])

      val orClauses = flatten(pFilter.get.asInstanceOf[Operators.Or])
      orClauses must haveLength(2)

      val longs = orClauses.map { orClause =>
        orClause must beAnInstanceOf[Operators.And]

        val clauses = flatten(orClause.asInstanceOf[Operators.And])
        clauses must haveLength(2)

        val lt = clauses.collectFirst {
          case c: Operators.LtEq[java.lang.Long] if c.getColumn.getColumnPath.toDotString == "dtg" => c
        }
        val gt = clauses.collectFirst {
          case c: Operators.GtEq[java.lang.Long] if c.getColumn.getColumnPath.toDotString == "dtg" => c
        }

        (lt.map(_.getValue.longValue()).getOrElse(-1), gt.map(_.getValue.longValue()).getOrElse(-1))
      }

      val ltMillis = Converters.convert("2017-01-05T00:00:00.000Z", classOf[Date]).getTime
      val gtMillis = Converters.convert("2017-01-01T00:00:00.000Z", classOf[Date]).getTime

      // millis OR micros
      longs must containTheSameElementsAs(Seq((ltMillis, gtMillis), (ltMillis * 1000L, gtMillis * 1000L)))
    }

    "augment property equals column" >> {
      val (pFilter, gFilter) =
        convert("name = 'foo' AND dtg BETWEEN '2017-01-01T00:00:00.000Z' AND '2017-01-05T00:00:00.000Z'")
      gFilter must beNone
      pFilter must beSome(beAnInstanceOf[Operators.And])
      val clauses = flatten(pFilter.get.asInstanceOf[Operators.And])
      clauses must haveLength(2)

      val eq = clauses.collectFirst {
        case c: Operators.Eq[Binary] if c.getColumn.getColumnPath.toDotString == "name" => c
      }
      val longs = clauses.collectFirst {
        case c: Operators.Or =>
          val orClauses = flatten(c)
          orClauses must haveLength(2)
          orClauses.map { orClause =>
            orClause must beAnInstanceOf[Operators.And]

            val clauses = flatten(orClause.asInstanceOf[Operators.And])
            clauses must haveLength(2)

            val lt = clauses.collectFirst {
              case c: Operators.LtEq[java.lang.Long] if c.getColumn.getColumnPath.toDotString == "dtg" => c
            }
            val gt = clauses.collectFirst {
              case c: Operators.GtEq[java.lang.Long] if c.getColumn.getColumnPath.toDotString == "dtg" => c
            }

            (lt.map(_.getValue.longValue()).getOrElse(-1), gt.map(_.getValue.longValue()).getOrElse(-1))
          }
      }

      eq.map(_.getValue) must beSome(Binary.fromString("foo"))

      val ltMillis = Converters.convert("2017-01-05T00:00:00.000Z", classOf[Date]).getTime
      val gtMillis = Converters.convert("2017-01-01T00:00:00.000Z", classOf[Date]).getTime

      // millis OR micros
      longs.getOrElse(Seq.empty) must containTheSameElementsAs(Seq((ltMillis, gtMillis), (ltMillis * 1000L, gtMillis * 1000L)))
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
