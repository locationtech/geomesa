/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.geotools.api.data.FeatureWriter
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.geotools.FastSettableFeatureWriter
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.geotools.converters.FastConverter

/**
 * Helper for simplifying the logic around writing to a GeoTools feature writer
 */
trait FeatureWriterHelper {

  /**
   * Write a feature to the underlying feature writer
   *
   * @param sf feature to write
   * @return feature that was written
   */
  def write(sf: SimpleFeature): SimpleFeature
}

object FeatureWriterHelper {

  /**
   * Gets a feature writer helper
   *
   * @param writer feature writer
   * @param useProvidedFids use fids from the input features - not necessary if provided fid hints are already set
   */
  def apply(writer: FeatureWriter[SimpleFeatureType, SimpleFeature], useProvidedFids: Boolean = false): FeatureWriterHelper = {
    writer match {
      case w: FastSettableFeatureWriter => new FastHelper(w, useProvidedFids)
      case _ => new UnoptimizedHelper(writer, useProvidedFids)
    }
  }

  /**
   * Helper class to populate a feature writer in an optimized way - avoids repeated lookups of the attribute type
   * bindings.
   *
   * @param writer writer
   * @param useProvidedFid use fids from the input feature - not necessary if provided fid hints are already set
   */
  private class FastHelper(writer: FastSettableFeatureWriter, useProvidedFid: Boolean) extends FeatureWriterHelper {

    import scala.collection.JavaConverters._

    private val bindings =
      writer.getFeatureType.getAttributeDescriptors.asScala.map(_.getType.getBinding.asInstanceOf[Class[AnyRef]]).toArray

    override def write(sf: SimpleFeature): SimpleFeature = {
      val next = writer.next()
      var i = 0
      while (i < bindings.length) {
        next.setAttributeNoConvert(i, FastConverter.convert(sf.getAttribute(i), bindings(i)))
        i += 1
      }
      next.getUserData.putAll(sf.getUserData)
      next.setId(sf.getID)
      if (useProvidedFid) {
        next.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      }
      writer.write()
      next
    }
  }

  /**
   * Unoptimized helper implementation, for non-geomesa writers
   *
   * @param writer writer
   * @param useProvidedFids use fids from the input feature - not necessary if provided fid hints are already set
   */
  private class UnoptimizedHelper(writer: FeatureWriter[SimpleFeatureType, SimpleFeature], useProvidedFids: Boolean)
      extends FeatureWriterHelper {
    override def write(sf: SimpleFeature): SimpleFeature = FeatureUtils.write(writer, sf, useProvidedFids)
  }
}
