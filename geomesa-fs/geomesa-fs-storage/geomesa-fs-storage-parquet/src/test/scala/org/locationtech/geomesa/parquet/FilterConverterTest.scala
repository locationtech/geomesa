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
import org.locationtech.geomesa.fs.storage.parquet.FilterConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.util.Date

class FilterConverterTest extends SpecificationWithJUnit {

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
    "convert point geo filter to z range lt/gt" in {
      val (pFilter, gFilter) = convert("bbox(geom, -24.0, -25.0, -18.0, -19.0)")
      gFilter must beSome(beAnInstanceOf[BBOX])
      pFilter must beSome(beAnInstanceOf[Operators.Or])

      // z ranges OR
      val orClauses = flatten(pFilter.get.asInstanceOf[Operators.Or])
      orClauses must not(beEmpty)

      val andClauses = orClauses.collect { case and: Operators.And => flatten(and) }
      andClauses must haveLength(orClauses.length)

      foreach(andClauses) { clause =>
        clause must haveLength(2)
        val lt = clause.collectFirst {
          case c: Operators.LtEq[Binary] if c.getColumn.getColumnPath.toDotString == "__geom_z2__" => c.getValue
        }
        val gt = clause.collectFirst {
          case c: Operators.GtEq[Binary] if c.getColumn.getColumnPath.toDotString == "__geom_z2__" => c.getValue
        }
        foreach(Seq(lt, gt))(_ must beSome)
      }
    }

    "convert non-point geo filter to z range lt/gt" in {
      val (pFilter, gFilter) = convert("bbox(line, -24.0, -25.0, -18.0, -19.0)")
      gFilter must beSome(beAnInstanceOf[BBOX])
      pFilter must beSome(beAnInstanceOf[Operators.Or])

      // z ranges OR
      val orClauses = flatten(pFilter.get.asInstanceOf[Operators.Or])
      orClauses must not(beEmpty)

      val andClauses = orClauses.collect { case and: Operators.And => flatten(and) }
      andClauses must haveLength(orClauses.length)

      foreach(andClauses) { clause =>
        clause must haveLength(2)
        val lt = clause.collectFirst {
          case c: Operators.LtEq[Binary] if c.getColumn.getColumnPath.toDotString == "__line_xz2__" => c.getValue
        }
        val gt = clause.collectFirst {
          case c: Operators.GtEq[Binary] if c.getColumn.getColumnPath.toDotString == "__line_xz2__" => c.getValue
        }
        foreach(Seq(lt, gt))(_ must beSome)
      }
    }

    "convert dtg ranges to long ranges" in {
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

      val ltMicros = Converters.convert("2017-01-05T00:00:00.000Z", classOf[Date]).getTime * 1000L
      val gtMicros = Converters.convert("2017-01-01T00:00:00.000Z", classOf[Date]).getTime * 1000L

      lt.map(_.getValue) must beSome(Long.box(ltMicros))
      gt.map(_.getValue) must beSome(Long.box(gtMicros))
    }

    "augment property equals column" in {
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

      val ltMicros = Converters.convert("2017-01-05T00:00:00.000Z", classOf[Date]).getTime * 1000L
      val gtMicros = Converters.convert("2017-01-01T00:00:00.000Z", classOf[Date]).getTime * 1000L

      lt.map(_.getValue) must beSome(Long.box(ltMicros))
      gt.map(_.getValue) must beSome(Long.box(gtMicros))
    }

    "query with an int" in {
      val (pFilter, gFilter) = convert("age = 20")
      gFilter must beNone
      pFilter must beSome(beAnInstanceOf[Operators.Eq[java.lang.Integer]])
      pFilter.get.asInstanceOf[Operators.Eq[java.lang.Integer]].getColumn.getColumnPath.toDotString mustEqual "age"
      pFilter.get.asInstanceOf[Operators.Eq[java.lang.Integer]].getValue.intValue() mustEqual 20
    }
  }
}
