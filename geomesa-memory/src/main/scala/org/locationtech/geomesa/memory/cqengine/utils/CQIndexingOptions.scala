/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType.CQIndexType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor

import scala.util.Try

// See geomesa/geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/geotools/Conversions.scala
object CQIndexingOptions extends LazyLogging {
  def getCQIndexType(ad: AttributeDescriptor): CQIndexType = {
    Option(ad.getUserData.get(SimpleFeatureTypes.OPT_CQ_INDEX).asInstanceOf[String])
      .flatMap(c => Try(CQIndexType.withName(c)).toOption).getOrElse(CQIndexType.NONE)
  }

  def setCQIndexType(ad: AttributeDescriptor, indexType: CQIndexType) {
    ad.getUserData.put(SimpleFeatureTypes.OPT_CQ_INDEX, indexType.toString)
  }
}

object CQIndexType extends Enumeration {
  type CQIndexType = Value
  val DEFAULT   = Value("default")   // Let GeoMesa pick.
  val NAVIGABLE = Value("navigable") // Use for numeric fields and Date?
  val RADIX     = Value("radix")     // Use for strings

  val UNIQUE    = Value("unique")    // Use only for unique fields; could be string, Int, Long
  val HASH      = Value("hash")      // Use for 'enumerated' strings

  val NONE      = Value("none")
}
