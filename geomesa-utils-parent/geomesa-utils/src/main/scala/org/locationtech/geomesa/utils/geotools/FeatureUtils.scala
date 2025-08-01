/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.api.data.FeatureWriter
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.util.factory.Hints

import java.lang.{Boolean => jBoolean}
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet

/** Utilities for re-typing and re-building [[SimpleFeatureType]]s and [[SimpleFeature]]s while
  * preserving user data which the standard Geo Tools utilities do not do.
  */
object FeatureUtils {

  // sourced from the following:
  //   https://github.com/geotools/geotools/blob/master/modules/library/cql/src/main/jjtree/ECQLGrammar.jjt
  //   http://docs.geotools.org/latest/userguide/library/cql/internal.html
  val ReservedWords: Set[String] = HashSet(
    "AFTER",
    "AND",
    "BEFORE",
    "BEYOND",
    "CONTAINS",
    "CROSSES",
    "DISJOINT",
    "DOES-NOT-EXIST",
    "DURING",
    "DWITHIN",
    "EQUALS",
    "EXCLUDE",
    "EXISTS",
    "FALSE",
    "GEOMETRYCOLLECTION",
    "ID",
    "INCLUDE",
    "INTERSECTS",
    "IS",
    "LIKE",
    "LINESTRING",
    "MULTILINESTRING",
    "MULTIPOINT",
    "MULTIPOLYGON",
    "NOT",
    "NULL",
    "OR",
    "OVERLAPS",
    "POINT",
    "POLYGON",
    "RELATE",
    "TOUCHES",
    "TRUE",
    "WITHIN"
  )

  /** Retypes a [[SimpleFeatureType]], preserving the user data.
   *
   * @param orig the original
   * @param propertyNames the property names for the new type
   * @return the new [[SimpleFeatureType]]
   */
  def retype(orig: SimpleFeatureType, propertyNames: Array[String]): SimpleFeatureType = {
    val mod = SimpleFeatureTypeBuilder.retype(orig, propertyNames: _*)
    mod.getUserData.putAll(orig.getUserData)
    mod
  }

  /** Retypes a [[SimpleFeature]], preserving the user data.
    *
    * @param orig the source feature
    * @param targetType the target type
    * @return the new [[SimpleFeature]]
    */
  def retype(orig: SimpleFeature, targetType: SimpleFeatureType): SimpleFeature = {
    val mod = DataUtilities.reType(targetType, orig, false)
    mod.getUserData.putAll(orig.getUserData)
    mod
  }

  /** A new [[SimpleFeatureType]] builder initialized with ``orig`` which, when ``buildFeatureType`` is
    * called will, as a last step, add all user data from ``orig`` to the newly built [[SimpleFeatureType]].
    *
    * @param orig the source feature
    * @return a new [[SimpleFeatureTypeBuilder]]
    */
  def builder(orig: SimpleFeatureType): SimpleFeatureTypeBuilder = {
    val builder = new SimpleFeatureTypeBuilder() {

      override def buildFeatureType(): SimpleFeatureType = {
        val result = super.buildFeatureType()
        result.getUserData.putAll(orig.getUserData)
        result
      }
    }

    builder.init(orig)
    builder
  }

  @deprecated("use `write` or `copyToFeature` instead")
  def copyToWriter(writer: FeatureWriter[SimpleFeatureType, SimpleFeature],
                   sf: SimpleFeature,
                   useProvidedFid: Boolean = false): SimpleFeature = {
    copyToFeature(writer.next(), sf, useProvidedFid)
  }

  /**
   * Copy a feature to the feature returned by a feature writer
   *
   * @param toWrite feature writer feature
   * @param sf feature containing the data we want to write
   * @param useProvidedFid use the feature id from the feature, or generate a new one
   * @return
   */
  def copyToFeature(toWrite: SimpleFeature, sf: SimpleFeature, useProvidedFid: Boolean = false): SimpleFeature = {
    var i = 0
    while (i < sf.getAttributeCount) {
      toWrite.setAttribute(i, sf.getAttribute(i))
      i += 1
    }
    toWrite.getUserData.putAll(sf.getUserData)
    if (useProvidedFid || jBoolean.TRUE == sf.getUserData.get(Hints.USE_PROVIDED_FID).asInstanceOf[jBoolean]) {
      toWrite.getIdentifier match {
        case id: FeatureIdImpl => id.setID(sf.getID)
        case _ => toWrite.getUserData.put(Hints.PROVIDED_FID, sf.getID)
      }
      toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    }
    toWrite
  }

  /**
    * Write a feature to a feature writer
    *
    * @param writer feature writer
    * @param sf feature to write
    * @param useProvidedFid use provided fid
    */
  def write(
      writer: FeatureWriter[SimpleFeatureType, SimpleFeature],
      sf: SimpleFeature,
      useProvidedFid: Boolean = false): SimpleFeature = {
    val written = writer.next()
    copyToFeature(written, sf, useProvidedFid)
    writer.write()
    written
  }

  /**
    *
    * @param sft simple feature type
    * @return
    */
  def sftReservedWords(sft: SimpleFeatureType): Seq[String] =
    sft.getDescriptors.asScala.map(_.getName.getLocalPart.toUpperCase(Locale.US)).filter(ReservedWords.contains).toList
}
