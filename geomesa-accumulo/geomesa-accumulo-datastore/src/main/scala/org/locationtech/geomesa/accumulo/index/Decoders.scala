/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.index

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.Key
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.text.WKBUtils

trait Decoder[T] {
  def decode(key: Key): T
}

abstract class ExtractingDecoder[T] extends Decoder[T] {
  def seqExtract(seq: Seq[TextExtractor], key: Key): String =
    seq.map { _.extract(key) }.mkString
}

case class GeohashDecoder(orderedSeq: Seq[TextExtractor]) extends ExtractingDecoder[GeoHash] {
  def decode(key: Key): GeoHash =
    GeoHash(seqExtract(orderedSeq, key).takeWhile(c => c != '.'))
}

case class DateDecoder(orderSeq: Seq[TextExtractor], fs: String) extends ExtractingDecoder[DateTime] {
  DateTimeZone.setDefault(DateTimeZone.forID("UTC"))
  val parser = org.joda.time.format.DateTimeFormat.forPattern(fs)
  def decode(key: Key): DateTime = parser.parseDateTime(seqExtract(orderSeq, key))
}

case class IdDecoder(orderedSeq: Seq[TextExtractor]) extends ExtractingDecoder[String] {
  def decode(key: Key): String = seqExtract(orderedSeq, key).replaceAll("_+$", "")
}

case class GeometryDecoder(orderedSeq: Seq[ColumnQualifierExtractor]) extends ExtractingDecoder[Geometry] {
  def decode(key: Key): Geometry =
    WKBUtils.read(seqExtract(orderedSeq, key).takeWhile(c => c != '.'))
}

