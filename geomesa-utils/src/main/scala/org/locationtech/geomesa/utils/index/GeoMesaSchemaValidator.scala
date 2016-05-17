/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.index

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.{DEFAULT_DATE_KEY, RichSimpleFeatureType}
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

object GeoMesaSchemaValidator {
  def validate(sft: SimpleFeatureType): Unit = {
    MixedGeometryCheck.validateGeometryType(sft)
    TemporalIndexCheck.validateDtgField(sft)
    ReservedWordCheck.validateAttributeNames(sft)
  }
}

/**
  * Utility object that ensures that none of the (local portion of the) property
  * names is a reserved word in ECQL.  Using those reserved words in a simple
  * feature type will cause queries to fail.
  */
object ReservedWordCheck extends LazyLogging {

  // ensure that no attribute names are reserved words within GeoTools that will cause query problems
  def validateAttributeNames(sft: SimpleFeatureType): Unit = {
    val reservedWords = FeatureUtils.sftReservedWords(sft)
    if (reservedWords.nonEmpty) {
      // TODO:  Make this an exception after GeoMesa 1.2.2
      logger.warn(
        "The simple feature type contains one or more attributes whose names are reserved words:  " +
          reservedWords.mkString(", "))
    }
  }

}

/**
 * Utility object for emitting a warning to the user if a SimpleFeatureType contains a temporal attribute, but
 * none is used in the index.
 *
 * Furthermore, this object presents a candidate to be used in this case.
 *
 * This is useful since the only symptom of this mistake is slower than normal queries on temporal ranges.
 */
object TemporalIndexCheck extends LazyLogging {

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

object MixedGeometryCheck extends LazyLogging {

  import java.lang.{Boolean => jBoolean}

  import SimpleFeatureTypes.MIXED_GEOMETRIES

  def validateGeometryType(sft: SimpleFeatureType): Unit = {
    val gd = sft.getGeometryDescriptor
    if (gd != null && gd.getType.getBinding == classOf[Geometry]) {
      val declared = sft.getUserData.get(MIXED_GEOMETRIES) match {
        case null => false
        case mixed: jBoolean if mixed => true
        case mixed: String if jBoolean.valueOf(mixed) => true
        case mixed if jBoolean.valueOf(mixed.toString) => true
        case _ => false
      }
      if (!declared) {
        throw new IllegalArgumentException("Trying to create a schema with mixed geometry type " +
            s"'${gd.getLocalName}:Geometry'. Queries may be slower when using mixed geometries. " +
            "If this is intentional, you may override this message by putting Boolean.TRUE into the " +
            s"SimpleFeatureType user data under the key '$MIXED_GEOMETRIES' before calling createSchema. " +
            "Otherwise, please specify a single geometry type (e.g. Point, LineString, Polygon, etc).")
      }
    }
  }
}