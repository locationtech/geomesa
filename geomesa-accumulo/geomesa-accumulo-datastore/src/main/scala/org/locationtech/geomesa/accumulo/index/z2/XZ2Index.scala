/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z2

import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.locationtech.geomesa.curve.XZ2SFC
import org.opengis.feature.simple.SimpleFeatureType

object XZ2Index extends AccumuloFeatureIndex with XZ2WritableIndex with XZ2QueryableIndex {

  val SFC = XZ2SFC(12)

  override val name: String = "xz2"

  override val version: Int = 1

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.nonPoints
  }
}
