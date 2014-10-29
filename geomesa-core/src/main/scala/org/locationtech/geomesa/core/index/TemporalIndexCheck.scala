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

package org.locationtech.geomesa.core.index

import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.core.index
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

/**
 * Utility object for emitting a warning to the user if a SimpleFeatureType contains a temporal attribute, but
 * none is used in the index.
 *
 * Furthermore, this object presents a candidate to be used in this case.
 *
 * This is useful since the only symptom of this mistake is slower than normal queries on temporal ranges.
 */

object TemporalIndexCheck extends Logging {
  def extractNewDTGFieldCandidate(sft: SimpleFeatureType): Option[String] = {
    //if the attribute is not actually present, look for one
    if (!index.getDtgDescriptor(sft).isDefined) {
      val dtgCandidates = scanForTemporalAttributes(sft)      // get all attributes which may be used
      emitDtgWarning(dtgCandidates)                           // emits a warning if candidates were found
      dtgCandidates.headOption                                // emits the first candidate or None
    }
    else None
  }

  def emitDtgWarning(matches: List[String]) {
    if (matches.nonEmpty) {
      lazy val theWarning =
        s"""
        |__________Possible problem detected in the SimpleFeatureType_____________
        |SF_PROPERTY_START_TIME points to no existing SimpleFeature attribute, or isn't defined.
        |However, the following attribute(s) could be used in GeoMesa's temporal index:
        |${matches.mkString("\n", "\n", "\n")}
        |Please note that while queries on a temporal attribute will still work,
        |queries will be faster if SF_PROPERTY_START_TIME, located in the SimpleFeatureType's UserData,
        |points to the attribute's name
        |
        |GeoMesa will now point SF_PROPERTY_START_TIME to the first temporal attribute found:
        |${matches.head}
        |so that the index will include a temporal component.
        |
      """.stripMargin
      logger.warn(theWarning)
    }
  }

  def scanForTemporalAttributes(sft: SimpleFeatureType) =
    sft.getAttributeDescriptors.asScala.toList
      .withFilter { classOf[java.util.Date] isAssignableFrom _.getType.getBinding } .map { _.getLocalName }
}