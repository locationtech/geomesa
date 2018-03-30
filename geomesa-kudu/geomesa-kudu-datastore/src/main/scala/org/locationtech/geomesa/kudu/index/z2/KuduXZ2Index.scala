/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index.z2

import org.locationtech.geomesa.index.index.z2._
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex

case object KuduXZ2Index extends KuduXZ2Index

trait KuduXZ2Index extends KuduFeatureIndex[XZ2IndexValues, Long] with KuduZ2Schema[XZ2IndexValues] {

  override val name: String = XZ2Index.Name

  override val version: Int = 1

  override protected val keySpace: XZ2IndexKeySpace = XZ2IndexKeySpace
}
