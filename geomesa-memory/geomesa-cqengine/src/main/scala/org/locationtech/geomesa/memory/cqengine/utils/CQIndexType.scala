/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions.OPT_CQ_INDEX
import org.opengis.feature.simple.SimpleFeatureType

object CQIndexType extends Enumeration {

  type CQIndexType = Value

  val DEFAULT   :Value = Value("default")   // let geomesa pick
  val NAVIGABLE :Value = Value("navigable") // use for numeric fields and date
  val RADIX     :Value = Value("radix")     // use for strings
  val UNIQUE    :Value = Value("unique")    // use only for unique fields; could be string, int, long
  val HASH      :Value = Value("hash")      // use for 'enumerated' strings
  val GEOMETRY  :Value = Value("geometry")  // use for geometries
  val NONE      :Value = Value("none")

  /**
    * Gets attributes configured at the simple feature type level
    *
    * @param sft simple feature type
    * @return
    */
  def getDefinedAttributes(sft: SimpleFeatureType): Seq[(String, CQIndexType)] = {
    import scala.collection.JavaConverters._

    val types = Seq.newBuilder[(String, CQIndexType)]
    types.sizeHint(sft.getAttributeCount)

    sft.getAttributeDescriptors.asScala.foreach { descriptor =>
      val opt = Option(descriptor.getUserData.get(OPT_CQ_INDEX).asInstanceOf[String])
      opt.map(CQIndexType.withName).filter(_ != NONE).foreach(t => types += descriptor.getLocalName -> t)
    }

    types.result()
  }
}
