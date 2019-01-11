/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.nio.ByteBuffer

import org.apache.avro.generic.{GenericData, GenericEnumSymbol, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._
import scala.util.parsing.combinator.RegexParsers

sealed trait AvroPath {
  def eval(record: AnyRef): Option[AnyRef]
}

object AvroPath extends RegexParsers {

  def apply(path: String): AvroPath = build(path)

  type Predicate[T] = T => Boolean

  case class PathExpr(field: String, pred: Predicate[GenericRecord] = _ => true) extends AvroPath {
    override def eval(record: AnyRef): Option[AnyRef] = {
      record match {
        case gr: GenericRecord =>
          gr.get(field) match {
            case x: GenericRecord     => Some(x).filter(pred)
            case x: Utf8              => Some(x.toString)
            case x: ByteBuffer        => Option(getArray(x))
            case x: GenericFixed      => Option(x.bytes())
            case x: GenericEnumSymbol => Option(x.toString)
            case x                    => Option(x)
          }

        case _ => Option(record)
      }
    }
  }

  case class UnionTypeFilter(n: String) extends Predicate[GenericRecord] {
    override def apply(v1: GenericRecord): Boolean = v1.getSchema.getName.equals(n)
  }

  case class ArrayRecordExpr(pred: Predicate[GenericRecord]) extends AvroPath {
    override def eval(r: AnyRef): Option[AnyRef] = r match {
      case a: GenericData.Array[GenericRecord] => a.find(pred)
    }
  }

  case class CompositeExpr(se: Seq[AvroPath]) extends AvroPath {
    override def eval(r: AnyRef): Option[AnyRef] = r match {
      case gr: GenericRecord => se.foldLeft[Option[AnyRef]](Some(gr))((acc, AvroPath) => acc.flatMap(AvroPath.eval))
      case _ => None
    }
  }

  private def build(s: String): AvroPath = parse(rep1(avroPath), s) match {
    case Success(t, _) => if (t.lengthCompare(1) == 0) { t.head } else { CompositeExpr(t) }
    case _ => null
  }

  private def avroPath = schemaTypeName | arrayRecord | pathExpr

  private def fieldName = "[A-Za-z0-9_]*".r
  private def field = "$" ~> fieldName
  private def pathExpr = "/" ~> fieldName ^^ {
    f => PathExpr(f)
  }

  private def schemaTypeName = (pathExpr <~ "$type=") ~ "[A-Za-z0-9_]+".r ^^ {
    case pe ~ stn => pe.copy(pred = UnionTypeFilter(stn))
  }

  private def arrayRecord = ("[$" ~> fieldName) ~ ("=" ~> "[A-Za-z0-9_]+".r) <~ "]" ^^ {
    case fn ~ fv =>
      ArrayRecordExpr(
        (gr) => gr.get(fn) match {
          case utf8string: Utf8 => utf8string.toString.equals(fv)
          case x => x.equals(fv)
        })
    case _ => ArrayRecordExpr(_ => false)
  }

  private def getArray(x: ByteBuffer): Array[Byte] = {
    val start = x.position
    val length = x.limit - start
    val bytes = Array.ofDim[Byte](length)
    x.get(bytes, 0, length)
    x.position(start)
    bytes
  }
}
