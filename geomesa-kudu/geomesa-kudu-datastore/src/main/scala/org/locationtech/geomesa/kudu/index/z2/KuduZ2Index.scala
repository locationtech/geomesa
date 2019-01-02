/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.kudu.index.z2

import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexKeySpace, Z2IndexValues}
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex

case object KuduZ2Index extends KuduZ2Index

trait KuduZ2Index extends KuduFeatureIndex[Z2IndexValues, Long] with KuduZ2Schema[Z2IndexValues] {

  override val name: String = Z2Index.Name

  override val version: Int = 1

  override protected val keySpace: Z2IndexKeySpace = Z2IndexKeySpace
}
