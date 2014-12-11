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

// This is the RasterIndexSchema, for more details see IndexSchema and SchemaHelpers

package org.locationtech.geomesa.raster.index

import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.raster.feature.Raster

case class RasterIndexSchema(encoder: RasterIndexEntryEncoder,
                             decoder: RasterIndexEntryDecoder) {

  def encode(raster: Raster, visibility: String = "") = encoder.encode(raster, visibility)
  def decode(entry: KeyValuePair): Raster = decoder.decode(entry)

  // utility method to ask for the maximum allowable shard number
  def maxShard: Int =
    encoder.rowf match {
      case CompositeTextFormatter(Seq(PartitionTextFormatter(numPartitions), xs@_*), sep) => numPartitions
      case _ => 1  // couldn't find a matching partitioner
    }

}

object RasterIndexSchema extends SchemaHelpers with Logging {

  // a key element consists of a separator and any number of random partitions, geohashes, and dates
  override def keypart: Parser[CompositeTextFormatter] =
    (sep ~ rep(randEncoder | geohashEncoder | dateEncoder | constantStringEncoder | resolutionEncoder | bandEncoder)) ^^
      {
        case sep ~ xs => CompositeTextFormatter(xs, sep)
      }

  // the column qualifier must end with an ID-encoder
  override def cqpart: Parser[CompositeTextFormatter] =
    phrase(sep ~ rep(randEncoder | geohashEncoder | dateEncoder | constantStringEncoder) ~ idEncoder) ^^ {
      case sep ~ xs ~ id => CompositeTextFormatter(xs :+ id, sep)
    }

  // An index key is three keyparts, one for row, colf, and colq
  override def formatter = keypart ~ PART_DELIMITER ~ keypart ~ PART_DELIMITER ~ cqpart ^^ {
    case rowf ~ PART_DELIMITER ~ cff ~ PART_DELIMITER ~ cqf => (rowf, cff, cqf)
  }

  def resolutionKeyPlanner: Parser[ResolutionPlanner] = resolutionPattern ^^ {
    case d => ResolutionPlanner(lexiDecodeStringToDouble(d))
  }

  def bandKeyPlanner: Parser[BandPlanner] = bandPattern ^^ {
    case b => BandPlanner(b)
  }

  def keyPlanner: Parser[KeyPlanner] =
    sep ~ rep(constStringPlanner | datePlanner | randPartitionPlanner | geohashKeyPlanner |
      resolutionKeyPlanner | bandKeyPlanner) <~ "::.*".r ^^ {
      case sep ~ list => CompositePlanner(list, sep)
    }

  def buildKeyPlanner(s: String) = parse(keyPlanner, s) match {
    case Success(result, _) => result
    case fail: NoSuccess => throw new Exception(fail.msg)
  }

  // builds the encoder from a string representation
  def buildKeyEncoder(s: String): RasterIndexEntryEncoder = {
    val (rowf, cff, cqf) = parse(formatter, s).get
    RasterIndexEntryEncoder(rowf, cff, cqf)
  }

  // builds a RasterIndexSchema
  def apply(s: String): RasterIndexSchema = {
    val keyEncoder     = buildKeyEncoder(s)
    val geohashDecoder = buildGeohashDecoder(s)
    val keyPlanner     = buildKeyPlanner(s) // TODO: establish if this is needed, or any Planner components?
    val indexEntryDecoder = RasterIndexEntryDecoder()
    RasterIndexSchema(keyEncoder, indexEntryDecoder)
  }

  def getRasterIndexEntryDecoder(s: String) = ??? // TODO: is this needed?

}

class RasterIndexSchemaBuilder(separator: String) {

  import org.locationtech.geomesa.raster.index.RasterIndexSchema._

  var newPart = true
  val schema = new StringBuilder()

  def randomNumber(maxValue: Int): RasterIndexSchemaBuilder = append(RANDOM_CODE, maxValue)
  def constant(constant: String): RasterIndexSchemaBuilder  = append(CONSTANT_CODE, constant)
  def date(format: String): RasterIndexSchemaBuilder        = append(DATE_CODE, format)

  def geoHash(offset: Int, length: Int): RasterIndexSchemaBuilder = append(GEO_HASH_CODE, offset, ',', length)

  def id(): RasterIndexSchemaBuilder = id(-1)
  def id(length: Int): RasterIndexSchemaBuilder = {
    if (length > 0) {
      append(ID_CODE, length)
    } else {
      append(ID_CODE)
    }
  }

  def resolution(): RasterIndexSchemaBuilder = append(RESOLUTION_CODE, 0.0)
  def resolution(res: Double): RasterIndexSchemaBuilder = append(RESOLUTION_CODE, lexiEncodeDoubleToString(res))
  def band(band: String): RasterIndexSchemaBuilder = append(BAND_CODE, band)

  def nextPart(): RasterIndexSchemaBuilder = {
    schema.append(PART_DELIMITER)
    newPart = true
    this
  }

  def build(): String = schema.toString()

  override def toString(): String = build

  def reset(): Unit = {
    schema.clear()
    newPart = true
  }

  /**
   * Wraps the code in the appropriate delimiters and adds the provided values
   *
   * @param code
   * @param values
   * @return
   */
  private def append(code: String, values: Any*): RasterIndexSchemaBuilder = {
    if (newPart) {
      schema.append(CODE_START).append(separator).append(CODE_END).append(SEPARATOR_CODE)
      newPart = false
    }
    schema.append(CODE_START)
    values.foreach(schema.append(_))
    schema.append(CODE_END).append(code)
    this
  }

}