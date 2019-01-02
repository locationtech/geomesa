/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import java.util.Date

import org.locationtech.geomesa.curve.BinnedTime.TimeToBinnedTime
import org.locationtech.geomesa.index.api.{KeyValue, WritableFeature}
import org.locationtech.geomesa.kudu.KuduValue
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema
import org.locationtech.geomesa.security.SecurityUtils
import org.opengis.feature.simple.SimpleFeature

class KuduWritableFeature(delegate: WritableFeature,
                          schema: KuduSimpleFeatureSchema,
                          dtgIndex: Option[Int],
                          toBin: TimeToBinnedTime) extends WritableFeature {

  override val feature: SimpleFeature = delegate.feature
  override val values: Seq[KeyValue] = delegate.values
  override val id: Array[Byte] = delegate.id

  val kuduValues: Seq[KuduValue[_]] = schema.serialize(feature)
  val vis: String = SecurityUtils.getVisibility(feature)

  val bin: Short = dtgIndex match {
    case None => 0
    case Some(i) =>
      val dtg = feature.getAttribute(i).asInstanceOf[Date]
      if (dtg == null) { 0 } else {
        toBin(dtg.getTime).bin
      }
  }
}
