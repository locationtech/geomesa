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
import org.locationtech.geomesa.index.api.WrappedFeature
import org.opengis.feature.simple.SimpleFeature

class KuduFeature(val feature: SimpleFeature, dtgIndex: Option[Int], toBin: TimeToBinnedTime) extends WrappedFeature {

  val bin: Short =
    toBin(dtgIndex.flatMap(i => Option(feature.getAttribute(i).asInstanceOf[Date]).map(_.getTime)).getOrElse(0L)).bin

  override def idBytes: Array[Byte] = throw new NotImplementedError()
}
