/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.util.ingest

import GDELT._
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import geomesa.core.index._
import geomesa.core.util.ingest.Ingestible
import org.joda.time.format.DateTimeFormat

/**
 * This file contains an example of how to ingest a file consisting of geo-
 * coded events from the global database of events, language, and tone
 * (GDELT) database.
 *
 * (See http://gdelt.utdallas.edu/data.html for a description of the data.)
 */

object GDELT {
  val gfWorld = new GeometryFactory(new PrecisionModel(), 4326)

  // date-time format
  val dtf = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC()

  // field separators
  val separator = "\t"
}


class GDELT extends Ingestible {
  // remember key field positions
  val idx_x = fields.indexWhere(_.name == "ActionGeo_Long")
  val idx_y = fields.indexWhere(_.name == "ActionGeo_Lat")
  val idx_t = fields.indexWhere(_.name == "SQLDATE")

  // called on every line in the source file
  def extractFieldValuesFromLine(line: String): Option[Map[String,Any]] = {
    try {
      // split this line into constituent fields
      val values = line.split(separator)
      if (values.size != attributeFields.size)
        throw new Exception(s"Skipping this line, because it has ${values.size} fields instead of ${attributeFields.size}}.")

      // wrap these attribute-values into a simple map
      val fieldsMap: Map[String,Any] =
        (0 until attributeFields.size).map(i => (attributeFields(i).name, values(i))).toMap

      // extract geometry
      val x = fieldsMap("ActionGeo_Long").toString.toDouble
      val y = fieldsMap("ActionGeo_Lat").toString.toDouble
      val point = gfWorld.createPoint(new Coordinate(x, y))

      // extract date-time
      val t = dtf.parseDateTime(fieldsMap("SQLDATE").toString)

      // wrap the values into a map
      Some(
        fieldsMap
          ++ Map(SF_PROPERTY_GEOMETRY -> point)
          ++ Map(SF_PROPERTY_START_TIME -> t.toDate)
          ++ Map(SF_PROPERTY_END_TIME -> t.toDate)
      )
    } catch {
      case e: MatchError => None
    }
  }
}