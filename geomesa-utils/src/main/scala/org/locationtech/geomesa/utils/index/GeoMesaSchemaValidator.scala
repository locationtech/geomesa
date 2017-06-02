/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs._
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object GeoMesaSchemaValidator {

  def validate(sft: SimpleFeatureType): Unit = {
    MixedGeometryCheck.validateGeometryType(sft)
    TemporalIndexCheck.validateDtgField(sft)
    TemporalIndexCheck.validateDtgIndex(sft)
    ReservedWordCheck.validateAttributeNames(sft)
    IndexConfigurationCheck.validateIndices(sft)
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
      val msg = "The simple feature type contains attribute name(s) that are reserved words: " +
          s"${reservedWords.mkString(", ")}. You may override this check by setting '$RESERVED_WORDS=true' " +
          "in the simple feature type user data, but it may cause errors with some functionality."
      if (SimpleFeatureTypes.toBoolean(sft.getUserData.get(RESERVED_WORDS))) {
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
      if (!SimpleFeatureTypes.toBoolean(sft.getUserData.get(IGNORE_INDEX_DTG))) {
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
  }

  // note: dtg should be set appropriately before calling this method
  def validateDtgIndex(sft: SimpleFeatureType): Unit = {
    sft.getDtgField.foreach { dtg =>
      if (sft.getDescriptor(dtg).getIndexCoverage == IndexCoverage.JOIN) {
        val declared = SimpleFeatureTypes.toBoolean(sft.getUserData.get(DEFAULT_DTG_JOIN))
        if (!declared) {
          throw new IllegalArgumentException("Trying to create a schema with a partial (join) attribute index " +
              s"on the default date field '$dtg'. This may cause whole-world queries with time bounds to be much " +
              "slower. If this is intentional, you may override this message by putting Boolean.TRUE into the " +
              s"SimpleFeatureType user data under the key '$DEFAULT_DTG_JOIN' before calling createSchema. " +
              "Otherwise, please either specify a full attribute index or remove it entirely.")
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
      val declared = SimpleFeatureTypes.toBoolean(sft.getUserData.get(MIXED_GEOMETRIES))
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

object IndexConfigurationCheck {

  def validateIndices(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    require(sft.getZShards > 0 && sft.getZShards < 128, "Z shards must be between 1 and 127")
    require(sft.getAttributeShards > 0 && sft.getAttributeShards < 128, "Attribute shards must be between 1 and 127")
  }
}