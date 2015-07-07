/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.{DEFAULT_DATE_KEY, RichSimpleFeatureType}
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

  def validateDtgField(sft: SimpleFeatureType): Unit = {
    val dtgCandidates = scanForTemporalAttributes(sft) // all attributes which may be used
    // validate that we have an ok field, and replace it if not
    if (!sft.getDtgField.exists(dtgCandidates.contains)) {
      sft.getDtgField.foreach { dtg => // clear out any existing invalid field
        logger.warn(s"Invalid date field '$dtg' specified for schema $sft")
        sft.clearDtgField()
      }
      // if there are valid fields, warn and set to the first available
      dtgCandidates.headOption.foreach { candidate =>
        lazy val theWarning = s"$DEFAULT_DATE_KEY is not valid or defined for simple feature type $sft. " +
            "However, the following attribute(s) can be used in GeoMesa's temporal index: " +
            s"${dtgCandidates.mkString(", ")}. GeoMesa will now point $DEFAULT_DATE_KEY to the first " +
            s"temporal attribute found: $candidate"
        logger.warn(theWarning)
        sft.setDtgField(candidate)
      }
    }
  }

  def scanForTemporalAttributes(sft: SimpleFeatureType) =
    sft.getAttributeDescriptors.asScala.toList
      .withFilter { classOf[java.util.Date] isAssignableFrom _.getType.getBinding } .map { _.getLocalName }
}