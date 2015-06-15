/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.scalding

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Range => AcRange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AcPair}
import org.apache.hadoop.io.Text
import org.apache.log4j.Level

import scala.util.parsing.combinator.JavaTokenParsers

sealed trait AccumuloSourceOptions {
  def instance: String
  def zooKeepers: String
  def user: String
  def password: String
  def table: String
  def logLevel: Option[Level]
  override def toString = s"${getClass.getSimpleName}[$instance,$table]"
}

case class AccumuloInputOptions(
    instance: String,
    zooKeepers: String,
    user: String,
    password: String,
    table: String,
    ranges: Seq[SerializedRange] = Seq.empty,
    columns: Seq[SerializedColumn] = Seq.empty,
    iterators: Seq[IteratorSetting] = Seq.empty,
    authorizations: Authorizations = new Authorizations(),
    autoAdjustRanges: Option[Boolean] = None,
    localIterators: Option[Boolean] = None,
    offlineTableScan: Option[Boolean] = None,
    scanIsolation: Option[Boolean] = None,
    logLevel: Option[Level] = None) extends AccumuloSourceOptions

object AccumuloInputOptions {
  def apply(dsParams: Map[String, String]) = {
    new AccumuloInputOptions(
      dsParams.getOrElse("instanceId", "None"),
      dsParams.getOrElse("zookeepers", "None"),
      dsParams.getOrElse("user", "None"),
      dsParams.getOrElse("password", "None"),
      dsParams.getOrElse("tableName", "None"))
  }
}

case class AccumuloOutputOptions(
    instance: String,
    zooKeepers: String,
    user: String,
    password: String,
    table: String,
    threads: Option[Int] = None,
    memory: Option[Long] = None,
    createTable: Boolean = false,
    logLevel: Option[Level] = None) extends AccumuloSourceOptions

object AccumuloOutputOptions {
  def apply(dsParams: Map[String, String]) = {
    new AccumuloOutputOptions(
      dsParams.getOrElse("instanceId", "None"),
      dsParams.getOrElse("zookeepers", "None"),
      dsParams.getOrElse("user", "None"),
      dsParams.getOrElse("password", "None"),
      dsParams.getOrElse("tableName", "None"),
      createTable = true)
  }
}

case class SerializedRange(start: Endpoint, end: Endpoint)

case class Endpoint(r: Option[String], cf: Option[String], cq: Option[String], inclusive: Boolean = false)

case class SerializedColumn(cf: String, cq: String)

object SerializedRange {

  private class RangeParser extends JavaTokenParsers {

    case class Row(r: String, cf: Option[String], cq: Option[String])
    case class Bracket(inclusive: Boolean = true)

    /**
     * Valid specs can have attributes that look like the following:
     * "id:Integer:opt1=v1,*geom:Geometry:srid=4326,ct:List[String]:index=true,mt:Map[String,Double]:index=false"
     * [row: abced cf: ppp cq:ab, row:dd cf:ee cq:zz)
     */

    def startBracket = """[\(\[]""".r ^^ {
      case b if b == "(" => Bracket(inclusive = false)
      case b if b == "[" => Bracket(inclusive = true)
    }

    def endBracket = """[\)\]]""".r ^^ {
      case b if b == ")" => Bracket(inclusive = false)
      case b if b == "]" => Bracket(inclusive = true)
    }

    def part = """[^\[\]\(\),\s]+""".r

    def row = part ~ part.? ~ part.? ^^ {
      case r ~ cf ~ cq => Row(r, cf, cq)
    }

    def separator = ","

    def start = startBracket ~ row.? ^^ {
      case b ~ r => Endpoint(r.map(_.r), r.flatMap(_.cf), r.flatMap(_.cq), b.inclusive)
    }

    def end = row.? ~ endBracket ^^ {
      case r ~ b => Endpoint(r.map(_.r), r.flatMap(_.cf), r.flatMap(_.cq), b.inclusive)
    }

    def range = start ~ separator ~ end ^^ {
      case s ~ _ ~ e => SerializedRange(s, e)
    }

    def ranges = repsep(range, separator)

    def parse(s: String): Seq[SerializedRange] = parse(ranges, s.trim) match {
      case Success(t, r) if r.atEnd => t
      case Error(msg, r)   => throw new IllegalArgumentException(msg)
      case Failure(msg, r) => throw new IllegalArgumentException(msg)
      case Success(t, r)   => throw new IllegalArgumentException(s"Malformed attribute in ${r.source} at ${r.pos}")
    }
  }

  def parse(s: String): Seq[SerializedRange] = new RangeParser().parse(s)

  def apply(s: String): Seq[SerializedRange] = parse(s)

  def apply(range: AcRange): Seq[SerializedRange] = apply(Seq(range))

  def apply(ranges: Seq[AcRange]): Seq[SerializedRange] = {
    val sb = new StringBuilder
    val strings = ranges.map { r =>
      sb.clear()
      if (r.isStartKeyInclusive) sb.append("[") else sb.append("(")
      if (!r.isInfiniteStartKey) {
        sb.append(r.getStartKey.getRow.toString)
        sb.append(" ").append(r.getStartKey.getColumnFamily.toString)
        sb.append(" ").append(r.getStartKey.getColumnQualifier.toString)
      }
      sb.append(",")
      if (!r.isInfiniteStopKey) {
        sb.append(r.getEndKey.getRow.toString)
        sb.append(" ").append(r.getEndKey.getColumnFamily.toString)
        sb.append(" ").append(r.getEndKey.getColumnQualifier.toString)
      }
      if (r.isEndKeyInclusive) sb.append("]") else sb.append(")")
      sb.toString()
    }
    apply(strings.mkString(","))
  }
}

object SerializedRangeSeq {

  def unapply(value: SerializedRange): Option[AcRange] = {
    val start = new Key(value.start.r.getOrElse(""), value.start.cf.getOrElse(""), value.start.cq.getOrElse(""))
    val end = new Key(value.end.r.getOrElse(""), value.end.cf.getOrElse(""), value.end.cq.getOrElse(""))
    Some(new AcRange(start, end, value.start.inclusive, value.end.inclusive, value.start.r.isEmpty, value.end.r.isEmpty))
  }
}

object SerializedColumn {

  private class ColumnParser extends JavaTokenParsers {

    def part = """[^\[\]\(\),\s]+""".r
    def pair = "[" ~> part ~ part <~ "]" ^^ {
      case cf ~ cq  => SerializedColumn(cf, cq)
    }

    def separator = ","

    def pairs = repsep(pair, separator)

    def parse(s: String): Seq[SerializedColumn] = parse(pairs, s.trim) match {
      case Success(t, r) if r.atEnd => t
      case Error(msg, r)   => throw new IllegalArgumentException(msg)
      case Failure(msg, r) => throw new IllegalArgumentException(msg)
      case Success(t, r)   => throw new IllegalArgumentException(s"Malformed attribute in ${r.source} at ${r.pos}")
    }
  }

  def parse(s: String): Seq[SerializedColumn] = new ColumnParser().parse(s)

  def apply(s: String): Seq[SerializedColumn] = parse(s)

  def apply(cols: AcPair[Text, Text]): Seq[SerializedColumn] = apply(Seq(cols))

  def apply(cols: Seq[AcPair[Text, Text]]): Seq[SerializedColumn] = {
    val strings = cols.map(c => s"[${c.getFirst.toString} ${c.getSecond.toString}]")
    apply(strings.mkString(","))
  }
}

object SerializedColumnSeq {

  def unapply(value: SerializedColumn): Option[AcPair[Text, Text]] =
    Some(new AcPair[Text, Text](new Text(value.cf), new Text(value.cq)))
}