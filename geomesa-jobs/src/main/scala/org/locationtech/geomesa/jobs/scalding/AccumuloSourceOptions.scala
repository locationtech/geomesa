/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.scalding

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mapreduce.lib.util.InputConfigurator
import org.apache.accumulo.core.data.{Range => AcRange, Key}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AcPair}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Level

import scala.collection.JavaConversions._
import scala.util.parsing.combinator.JavaTokenParsers

case class AccumuloSourceOptions(
    instance: String,
    zooKeepers: String,
    user: String,
    password: String,
    input: AccumuloInputOptions,
    output: AccumuloOutputOptions) {
  override val toString = s"AccumuloSourceOptions[$instance,$zooKeepers,$input,$output]"
}

case class AccumuloInputOptions(
    table: String,
    ranges: Seq[SerializedRange] = Seq.empty,
    columns: Seq[SerializedColumn] = Seq.empty,
    iterators: Seq[IteratorSetting] = Seq.empty,
    authorizations: Authorizations = new Authorizations(),
    autoAdjustRanges: Option[Boolean] = None,
    localIterators: Option[Boolean] = None,
    offlineTableScan: Option[Boolean] = None,
    scanIsolation: Option[Boolean] = None,
    logLevel: Option[Level] = None) {
  override val toString = s"AccumuloInputOptions[$table]"
}

case class AccumuloOutputOptions(
    table: String,
    threads: Option[Int] = None,
    memory: Option[Long] = None,
    createTable: Boolean = false,
    logLevel: Option[Level] = None) {
  override val toString = s"AccumuloOutputOptions[$table]"
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
      case b if b == "(" => Bracket(false)
      case b if b == "[" => Bracket(true)
    }

    def endBracket = """[\)\]]""".r ^^ {
      case b if b == ")" => Bracket(false)
      case b if b == "]" => Bracket(true)
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
        sb.append(r.getStartKey.getRow().toString)
        sb.append(" ").append(r.getStartKey.getColumnFamily().toString)
        sb.append(" ").append(r.getStartKey.getColumnQualifier().toString)
      }
      sb.append(",")
      if (!r.isInfiniteStopKey) {
        sb.append(r.getEndKey.getRow().toString)
        sb.append(" ").append(r.getEndKey.getColumnFamily().toString)
        sb.append(" ").append(r.getEndKey.getColumnQualifier().toString)
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