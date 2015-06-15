/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

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
    WKBUtils.read(seqExtract(orderedSeq, key).takeWhile(_ != '.'))
}

