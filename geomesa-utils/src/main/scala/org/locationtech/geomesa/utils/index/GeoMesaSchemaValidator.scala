/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs._
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object GeoMesaSchemaValidator {

  def validate(sft: SimpleFeatureType): Unit = {
    MixedGeometryCheck.validateGeometryType(sft)
    TemporalIndexCheck.validateDtgField(sft)
    ReservedWordCheck.validateAttributeNames(sft)
    IndexConfigurationCheck.validateIndices(sft)
  }

  private [geomesa] def declared(sft: SimpleFeatureType, prop: String): Boolean =
    Option(sft.getUserData.get(prop)).orElse(SystemProperty(prop).option).exists(SimpleFeatureTypes.toBoolean)
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
      val msg = "The simple feature type contains attribute name(s) that are reserved words: " +
          s"${reservedWords.mkString(", ")}. You may override this check by putting Boolean.TRUE into the " +
          s"SimpleFeatureType user data under the key '$RESERVED_WORDS' before calling createSchema, or by " +
          s"setting the system property '$RESERVED_WORDS' to 'true', however it may cause errors with some functionality."
      if (GeoMesaSchemaValidator.declared(sft, RESERVED_WORDS)) {
        logger.warn(msg)
      } else {
        throw new IllegalArgumentException(msg)
      }
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
      if (!GeoMesaSchemaValidator.declared(sft, IGNORE_INDEX_DTG)) {
        dtgCandidates.headOption.foreach { candidate =>
          lazy val theWarning = s"$DEFAULT_DATE_KEY is not valid or defined for simple feature type $sft. " +
              "However, the following attribute(s) can be used in GeoMesa's temporal index: " +
              s"${dtgCandidates.mkString(", ")}. To disable temporal indexing, put Boolean.TRUE into the " +
              s"SimpleFeatureType user data under the key '$IGNORE_INDEX_DTG' before calling createSchema, or " +
              s"set the system property '$IGNORE_INDEX_DTG' to 'true'. GeoMesa will now point $DEFAULT_DATE_KEY " +
              s"to the first temporal attribute found: $candidate"
          logger.warn(theWarning)
          sft.setDtgField(candidate)
        }
      }
    }
  }

  private def scanForTemporalAttributes(sft: SimpleFeatureType): Seq[String] =
    sft.getAttributeDescriptors.asScala.toList
      .withFilter { classOf[java.util.Date] isAssignableFrom _.getType.getBinding } .map { _.getLocalName }
}

object MixedGeometryCheck extends LazyLogging {

  def validateGeometryType(sft: SimpleFeatureType): Unit = {
    val gd = sft.getGeometryDescriptor
    if (gd != null && gd.getType.getBinding == classOf[Geometry]) {
      if (!GeoMesaSchemaValidator.declared(sft, MIXED_GEOMETRIES)) {
        throw new IllegalArgumentException("Trying to create a schema with mixed geometry type " +
            s"'${gd.getLocalName}:Geometry'. Queries may be slower when using mixed geometries. " +
            "If this is intentional, you may override this check by putting Boolean.TRUE into the " +
            s"SimpleFeatureType user data under the key '$MIXED_GEOMETRIES' before calling createSchema, or by " +
            s"setting the system property '$MIXED_GEOMETRIES' to 'true'. Otherwise, please specify a single " +
            s"geometry type (e.g. Point, LineString, Polygon, etc).")
      }
    }
  }
}

object IndexConfigurationCheck {

  def validateIndices(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    require(sft.getZShards > 0 && sft.getZShards < 128, "Z shards must be between 1 and 127")
    require(sft.getAttributeShards > 0 && sft.getAttributeShards < 128, "Attribute shards must be between 1 and 127")
  }
}
