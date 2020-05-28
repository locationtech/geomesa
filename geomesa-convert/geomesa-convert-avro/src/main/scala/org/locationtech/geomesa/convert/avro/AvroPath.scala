/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.nio.ByteBuffer

import org.apache.avro.generic.{GenericArray, GenericEnumSymbol, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import org.locationtech.geomesa.utils.text.BasicParser
import org.parboiled.errors.ParsingException
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

sealed trait AvroPath {
  def eval(record: Any): Option[Any]
}

object AvroPath extends BasicParser {

  type AvroPredicate = GenericRecord => Boolean

  private val Parser = new AvroPathParser()

  def apply(path: String): AvroPath = parse(path)

  @throws(classOf[ParsingException])
  def parse(path: String, report: Boolean = true): AvroPath = {
    if (path == null) {
      throw new IllegalArgumentException("Invalid path string: null")
    }
    val runner = if (report) { ReportingParseRunner(Parser.path) } else { BasicParseRunner(Parser.path) }
    val parsing = runner.run(path)
    parsing.result.getOrElse(throw new ParsingException(s"Error parsing avro path: $path"))
  }

  private def convert(record: Any): Any = {
    record match {
      case x: Utf8              => x.toString
      case x: ByteBuffer        => convertBytes(x)
      case x: GenericFixed      => x.bytes()
      case x: GenericEnumSymbol => x.toString
      case x: GenericArray[Any] => convertList(x)
      case x                    => x
    }
  }

  private def convertBytes(x: ByteBuffer): Array[Byte] = {
    val start = x.position
    val length = x.limit - start
    val bytes = Array.ofDim[Byte](length)
    x.get(bytes, 0, length)
    x.position(start)
    bytes
  }

  private def convertList(list: java.util.List[Any]): java.util.List[Any] = {
    val result = new java.util.ArrayList[Any](list.size())
    val iter = list.iterator()
    while (iter.hasNext) {
      result.add(convert(iter.next()))
    }
    result
  }

  case class PathExpr(field: String, predicate: AvroPredicate) extends AvroPath {
    override def eval(record: Any): Option[Any] = {
      record match {
        case gr: GenericRecord =>
          gr.get(field) match {
            case x: GenericRecord => Some(x).filter(predicate)
            case x                => Option(convert(x))
          }

        case _ => None
      }
    }
  }

  case class ArrayRecordExpr(field: String, matched: String) extends AvroPath {

    import scala.collection.JavaConverters._

    override def eval(r: Any): Option[Any] = r match {
      case a: java.util.List[GenericRecord] => a.asScala.find(predicate)
      case _ => None
    }

    private def predicate(record: GenericRecord): Boolean = {
      record.get(field) match {
        case x: Utf8 => x.toString == matched
        case x       => x == matched
      }
    }
  }

  case class CompositeExpr(se: Seq[AvroPath]) extends AvroPath {
    override def eval(r: Any): Option[Any] = r match {
      case gr: GenericRecord => se.foldLeft[Option[Any]](Some(gr))((result, current) => result.flatMap(current.eval))
      case _ => None
    }
  }

  case class UnionTypeFilter(n: String) extends AvroPredicate {
    override def apply(v1: GenericRecord): Boolean = v1.getSchema.getName == n
  }

  class AvroPathParser extends BasicParser {

    import org.parboiled.scala._

    // full simple feature spec
    def path: Rule1[AvroPath] = rule("Path") {
      oneOrMore(pathExpression | arrayRecord) ~ EOI ~~> {
        paths => if (paths.lengthCompare(1) == 0) { paths.head } else { CompositeExpr(paths) }
      }
    }

    private def pathExpression: Rule1[PathExpr] = rule("PathExpression") {
      "/" ~ identifier ~ optional("$type=" ~ identifier) ~~> {
        (field, typed) => PathExpr(field, typed.map(UnionTypeFilter.apply).getOrElse(_ => true))
      }
    }

    private def arrayRecord: Rule1[ArrayRecordExpr] = rule("ArrayRecord") {
      ("[$" ~ identifier ~ "=" ~ identifier ~ "]") ~~> {
        (field, matched) => ArrayRecordExpr(field, matched)
      }
    }

    private def identifier: Rule1[String] = rule("Identifier") {
      oneOrMore(char | anyOf(".-")) ~> { s => s }
    }
  }
}
