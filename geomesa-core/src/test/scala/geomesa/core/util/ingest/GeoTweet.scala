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

import GeoTweet._
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import geomesa.core.index._
import geomesa.core.util.ingest.Ingestible
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

/**
 * This file contains an example of how to ingest a file consisting of geo-
 * coded Tweets, one JSON Tweet per line of the input file.
 *
 * NB:  This example class makes no attempt to ingest all fields; only a
 * select few have been cherry-picked for demonstration purposes.  It
 * stores the entire JSON object as a single field, again as an example.
 */

object GeoTweet {
  val FIELD_ID = "id_str"
  val FIELD_SCREEN_NAME = "screen_name"
  val FIELD_TEXT = "text"
  val FIELD_RETWEETED = "retweeted"
  val FIELD_RETWEET_COUNT = "retweet_count"
  val FIELD_FRIENDS_COUNT = "friends_count"

  val gfWorld = new GeometryFactory(new PrecisionModel(), 4326)

  // date-time format for parsing out tweet creation date
  val dtf = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY")

  // regular expressions for parsing single-line JSON tweets into field maps
  val GeoPointRE = """.*\"geo\":\{\"type\":\"Point\",\"coordinates\":\[([^,]+),([^\]]+)\].*""".r
  val IdRE = """.*\"id_str\":\"([^\"]+)\".*""".r
  val FriendsRE = """.*\"friends_count\":([^,]*),.*""".r
  val TextRE = """.*\"text\":\"([^\"]+)\".*""".r
  val UsernameRE = """.*\"screen_name\":\"([^\"]+)\".*""".r
  val RetweetedRE = """.*\"retweeted\":([a-zA-Z]+).*""".r
  val RetweetCountRE = """.*\"retweet_count\":([^,]*),.*""".r
  val DateRE = """^\{\"created_at\":\"([^\"]+)\".*""".r
}


class GeoTweet extends Ingestible {
  // called on every line in the source file
  def extractFieldValuesFromLine(line: String): Option[Map[String,Any]] = {
    try {
      // extract fields we care about
      val UsernameRE(username) = line
      val GeoPointRE(y,x) = line
      val IdRE(id) = line
      val FriendsRE(friends) = line
      val TextRE(text) = line
      val RetweetedRE(retweeted) = line
      val RetweetCountRE(retweetCount) = line
      val DateRE(dateStr) = line
      val json = line  // remember the entire JSON Tweet object

      val date = dtf.parseDateTime(dateStr).withZone(DateTimeZone.forID("UTC")).toDate

      Some(Map[String,Any](
        FIELD_ID -> id,
        FIELD_SCREEN_NAME -> username,
        FIELD_TEXT -> text,
        FIELD_RETWEETED -> new java.lang.Boolean(retweeted),
        FIELD_RETWEET_COUNT -> new java.lang.Long(retweetCount),
        FIELD_FRIENDS_COUNT -> new java.lang.Long(friends),
        SF_PROPERTY_GEOMETRY -> gfWorld.createPoint(new Coordinate(x.toDouble, y.toDouble)),
        SF_PROPERTY_START_TIME -> date,
        SF_PROPERTY_END_TIME -> date
      ))
    } catch {
      case e: MatchError => None
    }
  }

  // put your favorite filters here, if you have any
  override def areFieldsValid(fieldsOpt: Option[Map[String,Any]]): Boolean =
    super.areFieldsValid(fieldsOpt) && fieldsOpt.map(fields => {
      fields.get(FIELD_SCREEN_NAME).isDefined &&
        fields.get(FIELD_TEXT).isDefined
    }).getOrElse(false)
}